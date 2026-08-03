import { sb } from './core.js';
import { rand } from './util.js';

// ===================== WebRTC over Supabase Realtime Broadcast =====================
// The offer/answer/ICE handshake rides a Broadcast channel named after the room code, so
// there's no third-party signaling broker to go flaky and nothing to run ourselves. Once
// the peers connect, every byte of every file flows directly browser-to-browser over a
// DTLS-encrypted SCTP data channel — it never passes through Supabase or any other server.
//
// Optional TURN relay — fill this in to make transfers work on cellular / symmetric NAT
// (carrier-grade NAT). Short-lived creds from Metered / Cloudflare / Twilio, or self-host
// coturn. Empty = STUN-only: fine on Wi-Fi, sometimes fails on mobile networks.
const TURN = [];
// e.g. const TURN = [{ urls: ['turn:HOST:3478?transport=udp', 'turn:HOST:3478?transport=tcp'], username: 'u', credential: 'p' }];
const ICE = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:global.stun.twilio.com:3478' },
        ...TURN,
    ],
};

const myId = (crypto.randomUUID ? crypto.randomUUID() : rand() + rand());

const emitter = () => {
    const L = {};
    return {
        on(ev, fn) { (L[ev] || (L[ev] = [])).push(fn); return this; },
        emit(ev, ...a) { (L[ev] || []).forEach((f) => { try { f(...a); } catch (e) { console.error(e); } }); },
    };
};

// A DTLS fingerprint appears in each side's SDP. Hashing both of them gives a short code
// that MUST match on both screens — the standard "short authentication string" check. If
// someone with the room code tried to sit in the middle, the two codes wouldn't agree.
const fpOf = (sdp) => { const m = /^a=fingerprint:\s*\S+\s+(\S+)/im.exec(sdp || ''); return m ? m[1].toUpperCase() : ''; };

const makeConn = (room, remote, cid, initiator, meta) => {
    const ev = emitter();
    const pc = new RTCPeerConnection(ICE);
    let dc = null, remoteSet = false, closed = false;
    const pend = [];

    const fireClose = (reason) => {
        if (closed) return;
        closed = true; api.open = false;
        room._conns.delete(cid);
        ev.emit('close', reason);
    };

    const api = {
        id: cid, peer: remote, meta, open: false,
        on(e, fn) { ev.on(e, fn); return api; },
        // Control messages are JSON strings; file bytes are raw ArrayBuffers. The channel
        // is ordered + reliable, so the two interleave predictably (see transfer.js).
        send(o) { try { if (dc && dc.readyState === 'open') { dc.send(JSON.stringify(o)); return true; } } catch (e) {} return false; },
        sendBinary(buf) { try { dc.send(buf); return true; } catch (e) { return false; } },
        close(reason) { try { dc && dc.close(); } catch (e) {} try { pc.close(); } catch (e) {} fireClose(reason || 'closed'); },
        get dataChannel() { return dc; },
        get bufferedAmount() { return dc ? dc.bufferedAmount : 0; },
        get maxMessage() { return (pc.sctp && pc.sctp.maxMessageSize) || 0; },
        async safetyCode() {
            const a = fpOf(pc.localDescription && pc.localDescription.sdp);
            const b = fpOf(pc.remoteDescription && pc.remoteDescription.sdp);
            if (!a || !b || !crypto.subtle) return '';
            const h = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode([a, b].sort().join('|'))));
            return String(((h[0] << 16) | (h[1] << 8) | h[2]) % 1000000).padStart(6, '0');
        },
        // Is this connection going through a TURN relay, or truly peer-to-peer? That's the
        // only distinction worth showing — and the two sides can classify the *same* link
        // differently (host vs srflx), so don't report anything finer or the labels disagree.
        async route() {
            try {
                const stats = await pc.getStats();
                let pair = null;
                stats.forEach((r) => { if (r.type === 'candidate-pair' && r.state === 'succeeded' && r.nominated !== false) pair = r; });
                if (!pair) return '';
                const local = stats.get(pair.localCandidateId), remoteC = stats.get(pair.remoteCandidateId);
                const relayed = (local && local.candidateType === 'relay') || (remoteC && remoteC.candidateType === 'relay');
                return relayed ? 'relay' : 'direct';
            } catch (e) { return ''; }
        },
    };

    const wireDC = (ch) => {
        dc = ch;
        dc.binaryType = 'arraybuffer';
        dc.onopen = () => { api.open = true; ev.emit('open'); };
        dc.onmessage = (m) => {
            if (typeof m.data === 'string') {
                let d; try { d = JSON.parse(m.data); } catch (e) { return; }
                ev.emit('data', d);
            } else ev.emit('chunk', m.data);
        };
        dc.onclose = () => fireClose('closed');
        dc.onerror = () => {};   // surfaced via connectionstatechange; a bare error event is noise
    };

    pc.onicecandidate = (e) => { if (e.candidate) room._sig(remote, { cid, ice: e.candidate }); };
    let discT = null;   // 'disconnected' is usually a transient blip — grace period before teardown
    pc.onconnectionstatechange = () => {
        const s = pc.connectionState;
        if (s === 'connected') { clearTimeout(discT); discT = null; }
        else if (s === 'disconnected') { clearTimeout(discT); discT = setTimeout(() => fireClose('dropped'), 10000); }
        else if (s === 'failed') { clearTimeout(discT); fireClose('failed'); }
        else if (s === 'closed') { clearTimeout(discT); fireClose('closed'); }
    };

    if (initiator) {
        wireDC(pc.createDataChannel('f', { ordered: true }));
        pc.createOffer()
            .then((o) => pc.setLocalDescription(o))
            .then(() => room._sig(remote, { cid, sdp: pc.localDescription, meta }))
            .catch(() => fireClose('failed'));
    } else {
        pc.ondatachannel = (e) => wireDC(e.channel);
    }

    room._conns.set(cid, {
        api,
        handleSignal: async (msg) => {
            try {
                if (msg.sdp) {
                    await pc.setRemoteDescription(msg.sdp);
                    remoteSet = true;
                    pend.splice(0).forEach((c) => pc.addIceCandidate(c).catch(() => {}));
                    if (msg.sdp.type === 'offer') {
                        await pc.setLocalDescription(await pc.createAnswer());
                        room._sig(remote, { cid, sdp: pc.localDescription });
                    }
                } else if (msg.ice) {
                    remoteSet ? await pc.addIceCandidate(msg.ice).catch(() => {}) : pend.push(msg.ice);
                }
            } catch (e) { fireClose('failed'); }
        },
    });
    return api;
};

// Join (or open) the room named by `code`.
//  - the SENDER waits for 'hello' announcements and dials each newcomer,
//  - the RECEIVER announces itself until someone dials back.
// Either way the sender is always the offerer, which keeps glare impossible.
const joinRoom = (code, { role, onConn, onStatus }) => {
    const room = {
        code, role, id: myId,
        _conns: new Map(),
        _ch: null,
        _sig(to, msg) {
            try { room._ch && room._ch.send({ type: 'broadcast', event: 'sig', payload: { to, from: myId, ...msg } }); } catch (e) {}
        },
        peers() { return [...room._conns.values()].map((c) => c.api); },
        leave() {
            clearInterval(helloT);
            room._conns.forEach((c) => c.api.close('left'));
            room._conns.clear();
            try { room._ch && sb.removeChannel(room._ch); } catch (e) {}
            room._ch = null;
        },
    };

    const ch = sb.channel(`chute-${code}`, {
        config: { broadcast: { self: false }, presence: { key: myId } },
    });
    room._ch = ch;

    ch.on('broadcast', { event: 'sig' }, ({ payload: p }) => {
        if (!p || p.to !== myId) return;
        let entry = room._conns.get(p.cid);
        if (!entry) {
            if (!p.sdp || p.sdp.type !== 'offer') return;   // stray candidate for a dead conn
            const c = makeConn(room, p.from, p.cid, false, p.meta);
            entry = room._conns.get(p.cid);
            onConn && onConn(c);
        }
        entry && entry.handleSignal(p);
    });

    // A receiver announcing itself. Only the sender answers. `hello` repeats every couple of
    // seconds until the receiver is connected, so dedupe by peer — but let a dial that never
    // completed be retried, otherwise one dropped offer strands them forever.
    const dialed = new Map();   // peer id -> time we last dialed
    ch.on('broadcast', { event: 'hello' }, ({ payload: p }) => {
        if (role !== 'send' || !p || !p.from || p.from === myId) return;
        const live = [...room._conns.values()].some((c) => c.api.peer === p.from);
        const last = dialed.get(p.from) || 0;
        if (live && Date.now() - last < 20000) return;
        dialed.set(p.from, Date.now());
        const c = makeConn(room, p.from, rand() + rand(), true, {});
        let everOpen = false;
        c.on('open', () => { everOpen = true; });
        c.on('close', () => { if (!everOpen) dialed.delete(p.from); });   // never connected → retry at once
        onConn && onConn(c);
    });

    ch.on('presence', { event: 'sync' }, () => {
        const n = Object.keys(ch.presenceState() || {}).length;
        onStatus && onStatus({ present: n });
    });

    let helloT = null, subscribed = false, wantAnnounce = (role === 'recv');
    const say = () => {
        // Quiet while we're actually talking to someone; starts up again by itself if that
        // connection drops, which is what makes an interrupted transfer reconnect.
        if ([...room._conns.values()].some((c) => c.api.open)) return;
        try { ch.send({ type: 'broadcast', event: 'hello', payload: { from: myId } }); } catch (e) {}
    };
    // The app turns announcing off once the transfer is finished or cancelled — otherwise a
    // completed receiver would keep advertising and get re-dialled forever.
    room.announce = (on) => {
        wantAnnounce = on;
        clearInterval(helloT);
        helloT = null;
        if (on && subscribed) { say(); helloT = setInterval(say, 2500); }
    };

    ch.subscribe((status) => {
        if (status === 'SUBSCRIBED') {
            subscribed = true;
            ch.track({ role, t: Date.now() });
            onStatus && onStatus({ joined: true });
            if (wantAnnounce) room.announce(true);
        } else if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') {
            onStatus && onStatus({ error: 'Lost the connection to the signaling channel.' });
        }
    });

    return room;
};

export { joinRoom, myId };
