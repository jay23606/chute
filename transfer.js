import { pickChunk, makeMeter, safeName, sum, throttle } from './util.js';

// ===================== the transfer protocol =====================
// One reliable, ordered data channel per peer carries both control messages (JSON strings)
// and file bytes (raw ArrayBuffer chunks). Because the channel preserves order across both,
// "here comes file X" → chunks → "that was all of X" is unambiguous with no framing of our
// own. Handshake, per file:
//
//   sender → { t:'manifest', files:[…] }     what's on offer
//   recv   → { t:'start', ids:[…] }          user picked a destination, go
//   sender → { t:'file', id, name, size }    next file
//   recv   → { t:'ready', id } | { t:'skip' } sink is open (no race with the first chunk)
//   sender → «binary chunks…» → { t:'end', id }
//   recv   → { t:'ok', id } | { t:'fail', id, msg }   flushed to disk
//   recv   → { t:'pause' } / { t:'resume' }   receiver-side flow control, any time
//   sender → { t:'done' }
//
// Nothing is ever buffered whole on either side: the sender slices the File on demand and
// the receiver streams to disk, so the only ceiling on file size is free disk space.

// Sender-side flow control: keep the SCTP send buffer fed but bounded.
const HIGH_WATER = 8 * 1024 * 1024;
const LOW_WATER = 1 * 1024 * 1024;
// Receiver-side: if the disk is slower than the network, tell the sender to hold off
// rather than piling ArrayBuffers up in memory.
const RECV_HIGH = 24 * 1024 * 1024;
const RECV_LOW = 4 * 1024 * 1024;

const READY_TIMEOUT = 120000;   // the receiver may be showing a file picker
const FLUSH_TIMEOUT = 300000;   // a multi-GB final flush to a slow disk

// Wait until the send buffer drains below the high-water mark. `bufferedamountlow` is
// edge-triggered, so a timeout backstop keeps us from parking forever if we miss the edge.
const drain = (dc) => new Promise((res) => {
    if (dc.readyState !== 'open' || dc.bufferedAmount <= HIGH_WATER) return res();
    const done = () => { clearTimeout(t); dc.removeEventListener('bufferedamountlow', done); res(); };
    const t = setTimeout(done, 500);
    dc.addEventListener('bufferedamountlow', done);
});

const fileId = (() => { let n = 0; return () => `f${++n}-${Math.random().toString(36).slice(2, 8)}`; })();

// ===================== sender =====================
// Holds File handles (never their contents) and serves them to every peer that connects.
// Several receivers can pull the same batch at once — each gets its own send loop.
const createSender = ({ onChange }) => {
    const files = [];              // { id, file, name, size, type }
    const peers = new Map();       // conn.id -> peer state
    let closed = false;
    const changed = () => { if (!closed) onChange && onChange(); };

    const manifest = () => ({ t: 'manifest', files: files.map((f) => ({ id: f.id, name: f.name, size: f.size, type: f.type })) });

    const addFiles = (list) => {
        const added = [];
        for (const file of list) {
            if (!file || typeof file.size !== 'number') continue;
            if (files.some((f) => f.name === file.name && f.size === file.size && f.file.lastModified === file.lastModified)) continue;
            const f = { id: fileId(), file, name: safeName(file.webkitRelativePath || file.name), size: file.size, type: file.type || '' };
            files.push(f);
            added.push(f);
        }
        if (added.length) {
            // Late arrivals: peers already connected get the updated list and can pull them too.
            peers.forEach((p) => p.conn.send(manifest()));
            changed();
        }
        return added;
    };

    const removeFile = (id) => {
        const i = files.findIndex((f) => f.id === id);
        if (i < 0) return;
        files.splice(i, 1);
        peers.forEach((p) => p.conn.send(manifest()));
        changed();
    };

    const settle = (p, key, msg) => { const w = p.waits.get(key); if (w) { p.waits.delete(key); w(msg); } };
    const waitFor = (p, key, ms) => new Promise((res, rej) => {
        const timer = setTimeout(() => { p.waits.delete(key); rej(new Error('timeout')); }, ms);
        p.waits.set(key, (msg) => { clearTimeout(timer); msg && msg.__closed ? rej(new Error('closed')) : res(msg); });
    });

    const pump = async (p) => {
        if (p.busy) return;
        p.busy = true;
        try {
            while (p.queue.length && p.conn.open) {
                const f = files.find((x) => x.id === p.queue[0]);
                p.queue.shift();
                if (!f) continue;

                p.cur = { id: f.id, name: f.name, size: f.size, sent: 0 };
                p.status = 'sending';
                changed();

                p.conn.send({ t: 'file', id: f.id, name: f.name, size: f.size, type: f.type });
                const ans = await waitFor(p, `ready:${f.id}`, READY_TIMEOUT);
                if (ans.t === 'skip') { p.skipped.add(f.id); p.cur = null; changed(); continue; }

                const dc = p.conn.dataChannel;
                dc.bufferedAmountLowThreshold = LOW_WATER;
                const chunk = pickChunk(p.conn.maxMessage);

                for (let off = 0; off < f.size; off += chunk) {
                    if (!p.conn.open) throw new Error('closed');
                    if (p.paused) await p.pauseGate;
                    await drain(dc);
                    if (!p.conn.open) throw new Error('closed');
                    // slice() is O(1) on a File — this reads just this window off disk, so a
                    // 50GB file costs the same memory as a 50MB one.
                    const buf = await f.file.slice(off, Math.min(off + chunk, f.size)).arrayBuffer();
                    if (!p.conn.sendBinary(buf)) throw new Error('send failed');
                    p.cur.sent += buf.byteLength;
                    p.tick();
                }

                p.conn.send({ t: 'end', id: f.id });
                const res = await waitFor(p, `ok:${f.id}`, FLUSH_TIMEOUT);
                if (res.t === 'fail') throw new Error(res.msg || 'the other side could not save it');
                p.done.add(f.id);
                p.sentBytes += f.size;
                p.cur = null;
                changed();
            }
            if (p.conn.open && !p.queue.length) {
                p.status = p.done.size ? 'done' : 'ready';
                p.conn.send({ t: 'done' });
                changed();
            }
        } catch (e) {
            if (p.conn.open) { p.status = 'error'; p.error = e.message; changed(); }
        }
        p.busy = false;
    };

    // Wire up a freshly connected receiver.
    const attach = (conn) => {
        const p = {
            conn, id: conn.id, status: 'connecting', error: '',
            queue: [], done: new Set(), skipped: new Set(), waits: new Map(),
            cur: null, sentBytes: 0, busy: false, paused: false, pauseGate: null, releaseGate: null,
            meter: makeMeter(), rate: 0, safety: '', route: '',
        };
        // Progress readout: bytes handed to SCTP minus what's still queued locally ≈ what
        // has actually gone out on the wire. Throttled — this fires per chunk, i.e. tens of
        // thousands of times a second on a fast local link.
        p.tick = throttle(() => {
            const onWire = Math.max(0, p.sentBytes + (p.cur ? p.cur.sent : 0) - conn.bufferedAmount);
            p.rate = p.meter.push(onWire, Date.now());
            p.onWire = onWire;
            changed();
        }, 100);
        p.total = () => sum(files, 'size');
        peers.set(conn.id, p);
        changed();

        conn.on('open', async () => {
            p.status = 'ready';
            conn.send(manifest());
            changed();
            p.safety = await conn.safetyCode();
            p.route = await conn.route();
            changed();
        });

        conn.on('data', (d) => {
            if (!d || !d.t) return;
            switch (d.t) {
                case 'start':
                    p.queue = (d.ids || []).filter((id) => files.some((f) => f.id === id) && !p.done.has(id));
                    p.meter.reset(Date.now());
                    pump(p);
                    break;
                case 'ready': case 'skip': settle(p, `ready:${d.id}`, d); break;
                case 'ok': case 'fail': settle(p, `ok:${d.id}`, d); break;
                case 'pause':
                    if (!p.paused) { p.paused = true; p.pauseGate = new Promise((r) => { p.releaseGate = r; }); }
                    break;
                case 'resume':
                    if (p.paused) { p.paused = false; p.releaseGate && p.releaseGate(); }
                    break;
                case 'bye': conn.close('bye'); break;
            }
        });

        conn.on('close', (reason) => {
            p.waits.forEach((w) => w({ __closed: true }));
            p.waits.clear();
            p.releaseGate && p.releaseGate();
            p.status = p.done.size && !p.queue.length && !p.cur ? 'done' : 'gone';
            p.reason = reason;
            changed();
            // Keep finished peers on screen as a receipt; drop ones that never got anywhere.
            if (!p.done.size) setTimeout(() => { peers.delete(conn.id); changed(); }, 4000);
        });
    };

    return {
        files, peers, addFiles, removeFile, attach,
        totalBytes: () => sum(files, 'size'),
        destroy() { closed = true; peers.forEach((p) => { p.conn.send({ t: 'bye' }); p.conn.close('bye'); }); peers.clear(); },
    };
};

// ===================== receiver =====================
const createReceiver = ({ conn, onChange }) => {
    const st = {
        status: 'connecting',      // connecting | offered | saving | done | error | gone
        files: [],                 // { id, name, size, type, status, recv }
        dest: null, error: '', safety: '', route: '',
        recvBytes: 0, rate: 0, meter: makeMeter(),
    };
    let sink = null, cur = null;
    let queue = Promise.resolve(), pending = 0, paused = false;
    const changed = () => onChange && onChange(st);
    const tick = throttle(changed, 100);   // per-chunk progress; see the sender's p.tick

    const total = () => sum(st.files.filter((f) => f.status !== 'skipped'), 'size');

    // Serialize writes onto one promise chain: chunks arrive faster than the disk accepts
    // them, and they must land in order.
    const enqueue = (buf) => {
        const f = cur;
        pending += buf.byteLength;
        if (!paused && pending > RECV_HIGH) { paused = true; conn.send({ t: 'pause' }); }
        const s = sink;
        queue = queue.then(async () => {
            try {
                if (!s || !f) return;                         // chunk for an aborted/skipped file
                await s.write(buf);
                f.recv += buf.byteLength;
                st.recvBytes += buf.byteLength;
                st.rate = st.meter.push(st.recvBytes, Date.now());
                tick();
            } finally {
                // Always give the credit back, or flow control deadlocks on the next error.
                pending -= buf.byteLength;
                if (paused && pending < RECV_LOW) { paused = false; conn.send({ t: 'resume' }); }
            }
        }).catch((e) => fail(f, e));
    };

    const fail = (f, e) => {
        if (f) { f.status = 'failed'; f.error = String(e && e.message || e); }
        st.status = 'error';
        st.error = String(e && e.message || e);
        conn.send({ t: 'fail', id: f && f.id, msg: st.error });
        changed();
    };

    // Called from a user gesture — the file/folder pickers demand one.
    const accept = async (chooser) => {
        const wanted = st.files.filter((f) => f.status === 'offered');
        if (!wanted.length) return;
        st.dest = await chooser({ fileCount: wanted.length, suggestedName: wanted[0].name });
        st.status = 'saving';
        st.meter.reset(Date.now());
        wanted.forEach((f) => { f.status = 'waiting'; });
        conn.send({ t: 'start', ids: wanted.map((f) => f.id) });
        changed();
    };

    conn.on('open', async () => {
        st.status = 'waiting-offer';
        changed();
        st.safety = await conn.safetyCode();
        st.route = await conn.route();
        changed();
    });

    conn.on('data', async (d) => {
        if (!d || !d.t) return;
        if (d.t === 'manifest') {
            const seen = new Map(st.files.map((f) => [f.id, f]));
            st.files = (d.files || []).map((f) => seen.get(f.id) || {
                id: f.id, name: safeName(f.name), size: Number(f.size) || 0, type: f.type || '',
                status: 'offered', recv: 0, error: '',
            });
            // The sender added files after we already picked a destination — take them too.
            if (st.dest && st.files.some((f) => f.status === 'offered')) {
                const extra = st.files.filter((f) => f.status === 'offered');
                extra.forEach((f) => { f.status = 'waiting'; });
                conn.send({ t: 'start', ids: extra.map((f) => f.id) });
            } else if (st.status === 'waiting-offer' || st.status === 'connecting') {
                st.status = 'offered';
            }
            st.total = total();
            changed();
            return;
        }
        if (d.t === 'file') {
            const f = st.files.find((x) => x.id === d.id);
            if (!f || !st.dest) { conn.send({ t: 'skip', id: d.id }); return; }
            try {
                await queue;                                  // let the previous file finish closing
                sink = await st.dest.open(f.name, f.size);
                cur = f;
                f.savedAs = sink.name;
                f.status = 'receiving';
                changed();
                conn.send({ t: 'ready', id: d.id });
            } catch (e) {
                sink = null; cur = null;
                f.status = 'failed'; f.error = String(e && e.message || e);
                conn.send({ t: 'skip', id: d.id, msg: f.error });
                changed();
            }
            return;
        }
        if (d.t === 'end') {
            const f = cur, s = sink;
            sink = null; cur = null;
            if (!f || !s) return;
            queue = queue.then(async () => {
                await s.close();
                f.status = f.recv === f.size ? 'saved' : 'partial';
                if (f.status === 'partial') f.error = 'incomplete';
                conn.send(f.status === 'saved' ? { t: 'ok', id: f.id } : { t: 'fail', id: f.id, msg: 'incomplete' });
                changed();
            }).catch((e) => fail(f, e));
            return;
        }
        if (d.t === 'done') {
            queue = queue.then(() => {
                if (st.status !== 'error' && st.files.some((f) => f.status === 'saved')) st.status = 'done';
                changed();
            });
            return;
        }
        if (d.t === 'bye') { conn.close('bye'); }
    });

    conn.on('chunk', (buf) => enqueue(buf));

    conn.on('close', (reason) => {
        queue = queue.then(async () => {
            if (sink) { await sink.abort().catch(() => {}); sink = null; }
            if (cur && cur.status === 'receiving') { cur.status = 'failed'; cur.error = 'connection lost'; }
            cur = null;
            if (st.status !== 'done' && st.status !== 'error') { st.status = 'gone'; st.reason = reason; }
            changed();
        });
    });

    return {
        state: st, accept,
        totalBytes: total,
        cancel() { conn.send({ t: 'bye' }); conn.close('bye'); },
    };
};

export { createSender, createReceiver, HIGH_WATER, RECV_HIGH };
