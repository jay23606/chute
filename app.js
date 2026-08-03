import { $, el, on, app, toast, copy, shareUrl, esc, fmtBytes, fmtRate, fmtDur,
    makeCode, normCode, isCode, sum, pct, throttle } from './core.js';
import { joinRoom } from './rtc.js';
import { createSender, createReceiver } from './transfer.js';
import { chooseDestination, destinationHint, canStreamToDisk, sweepOPFS, hasDirPicker } from './sink.js';

// ===================== app state =====================
// Exactly one of `sender` / `receiver` is live at a time; `room` is the signaling channel
// they share. Leaving a view tears all three down.
let room = null, sender = null, receiver = null, code = '', present = 0;

const teardown = () => {
    if (sender) { sender.destroy(); sender = null; }
    // Cancel rather than drop: it aborts the part-written file instead of leaving it staged.
    if (receiver) { receiver.cancel(); receiver = null; }
    if (room) { room.leave(); room = null; }
    code = ''; present = 0;
};

// The browser can't ask "are you sure?" without a reason — only nag when there's a transfer
// to lose. 'reconnecting' counts: the part-written file only survives while this tab lives.
const busy = () => !!(sender && [...sender.peers.values()].some((p) => p.status === 'sending'))
    || !!(receiver && ['saving', 'reconnecting'].includes(receiver.state.status));
window.addEventListener('beforeunload', (e) => { if (busy()) { e.preventDefault(); e.returnValue = ''; } });

// ===================== shared bits =====================
const bar = (frac, cls = '') => `<div class="bar ${cls}"><i style="width:${Math.max(0, Math.min(100, frac)).toFixed(1)}%"></i></div>`;

const fileRow = (f, right = '') => `
    <li class="frow">
        <span class="fname" title="${esc(f.name)}">${esc(f.name)}</span>
        <span class="fsize">${fmtBytes(f.size)}</span>
        ${right}
    </li>`;

const safetyBadge = (sas, route) => {
    if (!sas) return '';
    const how = route === 'relay' ? 'via relay' : route ? 'direct' : '';
    return `<span class="sas" title="Encrypted end to end. If this number matches on both screens, nobody is in the middle.">🔒 ${esc(sas)}${how ? ` · ${esc(how)}` : ''}</span>`;
};

// ===================== home =====================
const viewHome = () => {
    app().innerHTML = `
    <section class="hero">
        <h1>Send a big file to someone</h1>
        <p class="sub">Straight from your device to theirs. Nothing is uploaded, there's no size limit, and there's nothing to sign up for.</p>

        <div id="drop" class="drop" tabindex="0" role="button" aria-label="Choose files to send">
            <div class="dropicon">＋</div>
            <div class="dropmain">Drop files here</div>
            <div class="dropsub">or click to choose${hasDirPicker() ? ' — a whole folder works too' : ''}</div>
        </div>
        <div class="row center">
            <button class="ghost" id="pickfiles">Choose files</button>
            <button class="ghost" id="pickdir">Choose a folder</button>
        </div>

        <div class="sep"><span>or</span></div>

        <form class="joiner" id="joinform">
            <label for="joincode">Got a code?</label>
            <input id="joincode" name="code" placeholder="e.g. k7m2rp4t" autocomplete="off"
                   autocapitalize="off" spellcheck="false" inputmode="latin" maxlength="64">
            <button class="primary" type="submit">Receive</button>
        </form>
    </section>`;

    $('#drop').onclick = () => $('#picker').click();
    $('#drop').onkeydown = (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); $('#picker').click(); } };
    $('#pickfiles').onclick = () => $('#picker').click();
    $('#pickdir').onclick = () => $('#dirpicker').click();
    $('#joinform').onsubmit = (e) => {
        e.preventDefault();
        const c = normCode($('#joincode').value);
        if (!isCode(c)) return toast('That doesn’t look like a chute code.');
        location.hash = '#' + c;
    };
};

// ===================== sending =====================
const startSend = (files) => {
    if (!files || !files.length) return;
    teardown();
    code = makeCode();
    sender = createSender({ onChange: paintSend });
    sender.addFiles(files);
    room = joinRoom(code, {
        role: 'send',
        onConn: (c) => sender.attach(c),
        onStatus: (s) => { if (typeof s.present === 'number') { present = s.present; paintSend(); } if (s.error) toast(s.error); },
    });
    if (location.hash !== '#/send') location.hash = '#/send';
    else viewSend();
};

const viewSend = () => {
    if (!sender) return (location.hash = '#/');
    const link = shareUrl(code);
    app().innerHTML = `
    <section class="panel">
        <h2>Ready to send</h2>
        <p class="sub">Send this link (or read out the code) to whoever should get the files. Keep this tab open until they're done — the files are coming from <em>this</em> device.</p>

        <div class="codebox">
            <div class="code" id="bigcode">${esc(code)}</div>
            <div class="row">
                <button class="primary" id="copylink">Copy link</button>
                <button class="ghost" id="copycode">Copy code</button>
                <button class="ghost" id="qrbtn" aria-expanded="false">QR code</button>
                <button class="ghost" id="sharebtn" hidden>Share…</button>
            </div>
            <div class="link" id="linktext">${esc(link)}</div>
            <div id="qr" class="qr" hidden></div>
        </div>

        <h3 class="secttl">Files <span id="filecount" class="muted"></span></h3>
        <ul class="files" id="sendfiles"></ul>
        <div class="row">
            <button class="ghost" id="addmore">Add more files</button>
            <button class="ghost danger" id="stopall">Stop sharing</button>
        </div>

        <h3 class="secttl">Receivers</h3>
        <div id="peers"></div>
    </section>`;

    $('#copylink').onclick = async () => toast(await copy(link) ? 'Link copied.' : 'Couldn’t copy — select the link instead.');
    $('#copycode').onclick = async () => toast(await copy(code) ? 'Code copied.' : 'Couldn’t copy.');
    $('#addmore').onclick = () => $('#picker').click();
    $('#stopall').onclick = () => { teardown(); location.hash = '#/'; };
    $('#qrbtn').onclick = toggleQR;
    if (navigator.share) {
        const b = $('#sharebtn');
        b.hidden = false;
        b.onclick = () => navigator.share({ title: 'chute', text: 'Files for you', url: link }).catch(() => {});
    }
    on($('#sendfiles'), 'click', '[data-del]', (e, t) => sender.removeFile(t.dataset.del));
    paintSend();
};

let qrLoaded = null;
const toggleQR = async () => {
    const box = $('#qr'), btn = $('#qrbtn');
    if (!box.hidden) { box.hidden = true; btn.setAttribute('aria-expanded', 'false'); return; }
    box.hidden = false;
    btn.setAttribute('aria-expanded', 'true');
    if (box.dataset.done) return;
    box.textContent = 'Generating…';
    try {
        // Lazy — most sessions never open the QR, and it's the only extra dependency.
        qrLoaded = qrLoaded || import('https://esm.sh/qrcode-generator@1.4.4');
        const mod = await qrLoaded;
        const qrcode = mod.default || mod;
        const q = qrcode(0, 'M');
        q.addData(shareUrl(code));
        q.make();
        box.innerHTML = q.createSvgTag({ cellSize: 5, margin: 2, scalable: true });
        box.dataset.done = '1';
    } catch (e) {
        box.textContent = 'Couldn’t load the QR generator — use the link instead.';
    }
};

const peerCard = (p) => {
    const total = sender.totalBytes();
    const sent = Math.max(0, p.onWire || p.sentBytes || 0);
    const label = {
        connecting: 'Connecting…', ready: 'Connected — waiting for them to choose where to save',
        sending: `Sending ${p.cur ? esc(p.cur.name) : ''}`, done: 'All done',
        error: `Stopped: ${esc(p.error || 'something went wrong')}`,
        gone: p.done.size ? 'Disconnected' : 'Left before starting',
    }[p.status] || p.status;
    const stats = p.status === 'sending'
        ? `${fmtBytes(sent)} of ${fmtBytes(total)} · ${fmtRate(p.rate)} · ${fmtDur(p.meter.eta(total - sent))} left`
        : `${p.done.size} of ${sender.files.length} file${sender.files.length === 1 ? '' : 's'} delivered`;
    return `
    <div class="peer ${esc(p.status)}">
        <div class="peerhd"><strong>Receiver</strong>${safetyBadge(p.safety, p.route)}</div>
        <div class="peerst">${label}</div>
        ${bar(pct(sent, total), p.status === 'error' || p.status === 'gone' ? 'bad' : p.status === 'done' ? 'good' : '')}
        <div class="muted small">${stats}</div>
    </div>`;
};

const paintSend = throttle(() => {
    if (!sender || !$('#sendfiles')) return;
    $('#sendfiles').innerHTML = sender.files.map((f) =>
        fileRow(f, `<button class="x" data-del="${esc(f.id)}" aria-label="Remove ${esc(f.name)}">✕</button>`)).join('')
        || '<li class="empty">No files yet.</li>';
    $('#filecount').textContent = sender.files.length
        ? `— ${sender.files.length} file${sender.files.length === 1 ? '' : 's'}, ${fmtBytes(sender.totalBytes())}`
        : '';
    const peers = [...sender.peers.values()];
    $('#peers').innerHTML = peers.length
        ? peers.map(peerCard).join('')
        : `<div class="waiting"><span class="pulse"></span>Waiting for someone to open the link…${present > 1 ? ' (someone is here)' : ''}</div>`;
}, 100);

// ===================== receiving =====================
const startReceive = (c) => {
    teardown();
    code = c;
    room = joinRoom(code, {
        role: 'recv',
        onConn: (conn) => {
            // A connection arriving while we're mid-transfer is the sender coming back after
            // a drop — hand it to the existing receiver so it resumes instead of restarting.
            if (receiver && receiver.canRebind()) { receiver.rebind(conn); paintRecv(); return; }
            if (receiver) return conn.close('duplicate');   // one sender per room
            receiver = createReceiver({ conn, onChange: onRecvChange });
            paintRecv();
        },
        onStatus: (s) => { if (typeof s.present === 'number') { present = s.present; paintRecv(); } if (s.error) toast(s.error); },
    });
    viewRecv();
};

// Stop advertising ourselves once there's nothing left to receive — otherwise the sender
// would keep re-dialling a finished peer forever.
// Keep advertising ourselves while there's still something to receive — that's what lets a
// dropped transfer reconnect. Stop only when it's genuinely over, or the sender would keep
// re-dialling a finished peer forever.
const DONE_STATES = ['done', 'cancelled', 'error'];
const onRecvChange = (st) => {
    if (room) room.announce(!DONE_STATES.includes(st.status));
    paintRecv();
};

const viewRecv = () => {
    app().innerHTML = `
    <section class="panel">
        <h2 class="hdrow">Incoming <span id="rxsas"></span></h2>
        <div id="rxhead"></div>
        <ul class="files" id="rxfiles"></ul>
        <div id="rxfoot"></div>
    </section>`;
    paintRecv();
};

const rxStatusLine = (st) => {
    const totalB = receiver.totalBytes();
    switch (st.status) {
        case 'connecting': case 'waiting-offer':
            return `<div class="waiting"><span class="pulse"></span>Connecting to the sender…</div>`;
        case 'offered':
            return `<p class="sub">${st.files.length} file${st.files.length === 1 ? '' : 's'}, ${fmtBytes(totalB)} in total. ${esc(destinationHint(st.files.length, totalB))}</p>`;
        case 'saving': {
            const eta = st.meter.eta(totalB - st.recvBytes);
            return `${bar(pct(st.recvBytes, totalB))}
                <p class="muted small">${fmtBytes(st.recvBytes)} of ${fmtBytes(totalB)} · ${fmtRate(st.rate)} · ${fmtDur(eta)} left${st.resumes ? ` · resumed ${st.resumes}×` : ''}</p>`;
        }
        case 'reconnecting':
            return `${bar(pct(st.recvBytes, totalB), 'wait')}
                <div class="waiting"><span class="pulse"></span>Connection lost — waiting for the sender to come back.</div>
                <p class="muted small">Nothing you've already received is lost: ${fmtBytes(st.recvBytes)} of ${fmtBytes(totalB)} is saved, and the transfer will pick up from there.</p>`;
        case 'done':
            return `${bar(100, 'good')}<p class="ok">Done — ${fmtBytes(st.recvBytes)} saved.</p>`;
        case 'error':
            return `${bar(pct(st.recvBytes, totalB), 'bad')}<p class="bad">Stopped: ${esc(st.error)}</p>`;
        case 'gone':
            return `${bar(pct(st.recvBytes, totalB), 'bad')}<p class="bad">The sender disconnected before everything arrived. Ask them to reopen the link and try again.</p>`;
        default: return '';
    }
};

const RX_LABEL = { offered: '', waiting: 'waiting…', receiving: '', saved: '✓ saved', partial: 'incomplete', failed: 'failed', skipped: 'skipped' };

const paintRecv = throttle(() => {
    if (!$('#rxfiles')) return;
    if (!receiver) {
        $('#rxhead').innerHTML = `<div class="waiting"><span class="pulse"></span>Waiting for the sender…</div>
            <p class="sub">The person sending has to keep their chute tab open — the files come from their device.</p>`;
        $('#rxfiles').innerHTML = '';
        $('#rxfoot').innerHTML = `<div class="row"><a class="ghost" href="#/">Cancel</a></div>`;
        return;
    }
    const st = receiver.state;
    // The safety number is only worth anything if BOTH people can see it and compare.
    $('#rxsas').innerHTML = safetyBadge(st.safety, st.route);
    $('#rxhead').innerHTML = rxStatusLine(st);
    $('#rxfiles').innerHTML = st.files.map((f) => {
        const right = f.status === 'receiving'
            ? `<span class="fstat">${bar(pct(f.recv, f.size))}</span>`
            : `<span class="fstat ${f.status === 'saved' ? 'ok' : (f.status === 'failed' || f.status === 'partial') ? 'bad' : 'muted'}">${esc(f.error || RX_LABEL[f.status] || '')}</span>`;
        return fileRow(f, right);
    }).join('');

    const canSave = st.status === 'offered' && st.files.some((f) => f.status === 'offered');
    $('#rxfoot').innerHTML = canSave
        ? `<div class="row"><button class="primary big" id="saveall">Save ${st.files.length === 1 ? 'file' : `all ${st.files.length} files`}</button>
           <a class="ghost" href="#/">Cancel</a></div>
           ${!canStreamToDisk() && receiver.totalBytes() > 2e9 ? `<p class="warn">This browser has to buffer the whole thing in memory, which may fail at this size. Chrome or Edge writes it straight to disk.</p>` : ''}`
        : st.status === 'saving'
            ? `<div class="row"><button class="ghost danger" id="cancelrx">Cancel</button></div>`
            : st.status === 'reconnecting'
                ? `<div class="row"><button class="ghost" id="salvage">Stop waiting and keep what arrived</button>
                   <button class="ghost danger" id="cancelrx">Cancel</button></div>`
                : `<div class="row"><a class="ghost" href="#/">Back</a></div>`;

    const sa = $('#saveall');
    if (sa) sa.onclick = async () => {
        sa.disabled = true;
        try {
            await receiver.accept(chooseDestination);
        } catch (e) {
            sa.disabled = false;
            if (e && (e.name === 'AbortError' || e.name === 'NotAllowedError')) return;   // user closed the picker
            toast('Couldn’t start saving: ' + (e && e.message || e));
        }
    };
    const cx = $('#cancelrx');
    if (cx) cx.onclick = () => { receiver.cancel(); toast('Transfer cancelled.'); };
    const sv = $('#salvage');
    if (sv) sv.onclick = () => { receiver.salvage(); toast('Saved what arrived.'); };
}, 100);

// ===================== about =====================
const viewAbout = () => {
    app().innerHTML = `
    <section class="panel prose">
        <h2>How chute works</h2>
        <p>Your files never leave your device except to go straight to the person you're sending them to. There's no upload step and no copy sitting on a server, so there's no size limit beyond their free disk space.</p>
        <ol>
            <li><strong>You pick files.</strong> chute keeps a handle to them and reads them a slice at a time — a 50&nbsp;GB file uses no more memory than a 50&nbsp;MB one.</li>
            <li><strong>You get a code.</strong> It's in the part of the link after the <code>#</code>, which browsers never send to a server.</li>
            <li><strong>They open the link.</strong> The two browsers swap the handful of messages needed to find each other, then connect directly.</li>
            <li><strong>Bytes flow peer to peer</strong> over an encrypted channel, straight into a file on their disk.</li>
        </ol>
        <h3>How private is it?</h3>
        <p>The connection is encrypted (DTLS) and the file bytes only ever exist on the two devices. The room code is the key: anyone who has it can receive the files, so share it the way you'd share a password. Both screens show a six-digit safety number once connected — if they match, nobody is in the middle.</p>
        <h3>If the connection drops</h3>
        <p>Nothing is lost and nothing starts over. What's already been received stays on disk, and as soon as the two devices can see each other again the transfer picks up at the exact byte it stopped on. If the sender doesn't come back, you can keep the part of the file that did arrive.</p>
        <p>That holds as long as both tabs stay open. Closing the receiving tab does throw away a half-finished file.</p>
        <h3>Things to know</h3>
        <ul>
            <li>The sender's tab has to stay open for the whole transfer.</li>
            <li>Chrome and Edge stream straight to disk. Firefox and Safari stage the file in browser storage first, so they need roughly the file's size in free space before the download appears.</li>
            <li>Some mobile networks block direct connections between devices. On Wi-Fi it almost always works.</li>
        </ul>
        <p><a class="ghost" href="#/">Back</a></p>
    </section>`;
};

// ===================== router =====================
const route = () => {
    const raw = location.hash.replace(/^#/, '');
    if (raw === '/send') { sender ? viewSend() : (location.hash = '#/'); return; }
    if (raw === '/about') { teardown(); viewAbout(); return; }
    if (raw === '' || raw === '/') { teardown(); viewHome(); return; }
    const c = normCode(raw);
    if (isCode(c)) { startReceive(c); return; }
    location.hash = '#/';
};
window.addEventListener('hashchange', route);

// ===================== input plumbing =====================
// One file picker serves both "start a send" and "add more to the current send".
const gotFiles = (list) => {
    const files = [...list].filter((f) => f.size > 0);   // directories come through as 0-byte entries
    if (!files.length) return toast('No readable files in that selection.');
    if (sender) { sender.addFiles(files); toast(`Added ${files.length} file${files.length === 1 ? '' : 's'}.`); }
    else startSend(files);
};
$('#picker').onchange = (e) => { gotFiles(e.target.files); e.target.value = ''; };
$('#dirpicker').onchange = (e) => { gotFiles(e.target.files); e.target.value = ''; };

// Drag and drop anywhere on the page (except while receiving — nothing to do with it there).
let dragDepth = 0;
const dropOK = () => !receiver;
window.addEventListener('dragenter', (e) => { if (!dropOK()) return; e.preventDefault(); if (++dragDepth === 1) document.body.classList.add('dragging'); });
window.addEventListener('dragover', (e) => { if (dropOK()) e.preventDefault(); });
window.addEventListener('dragleave', () => { if (--dragDepth <= 0) { dragDepth = 0; document.body.classList.remove('dragging'); } });
window.addEventListener('drop', async (e) => {
    if (!dropOK()) return;
    e.preventDefault();
    dragDepth = 0;
    document.body.classList.remove('dragging');
    const dt = e.dataTransfer;
    if (!dt) return;
    const files = dt.items ? await filesFromItems(dt.items) : [...dt.files];
    gotFiles(files);
});

// A dropped folder arrives as a directory entry — walk it so "drop a folder" just works.
const filesFromItems = async (items) => {
    const entries = [...items].map((i) => (i.webkitGetAsEntry ? i.webkitGetAsEntry() : null));
    if (entries.some((en) => en && en.isDirectory)) {
        const out = [];
        const walk = async (entry, prefix) => {
            if (entry.isFile) {
                const f = await new Promise((res, rej) => entry.file(res, rej)).catch(() => null);
                if (f) { try { Object.defineProperty(f, 'webkitRelativePath', { value: prefix + f.name }); } catch (e) {} out.push(f); }
            } else if (entry.isDirectory) {
                const reader = entry.createReader();
                for (;;) {
                    const batch = await new Promise((res) => reader.readEntries(res, () => res([])));
                    if (!batch.length) break;                       // readEntries pages at 100
                    for (const c of batch) await walk(c, `${prefix}${entry.name}/`);
                }
            }
        };
        for (const en of entries) if (en) await walk(en, '');
        return out;
    }
    return [...items].filter((i) => i.kind === 'file').map((i) => i.getAsFile()).filter(Boolean);
};

// ===================== install prompt =====================
let installEvent = null;
window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    installEvent = e;
    $('#install').hidden = false;
});
$('#install').onclick = async () => {
    if (!installEvent) return;
    installEvent.prompt();
    await installEvent.userChoice.catch(() => {});
    installEvent = null;
    $('#install').hidden = true;
};

// ===================== share target =====================
// Files shared to chute from the OS share sheet are stashed by the service worker, which
// then redirects here with #/shared.
const takeShared = async () => {
    try {
        const reg = await navigator.serviceWorker.ready;
        const ch = new MessageChannel();
        const files = await new Promise((res) => {
            ch.port1.onmessage = (e) => res((e.data && e.data.files) || []);
            reg.active.postMessage({ t: 'take-shared' }, [ch.port2]);
            setTimeout(() => res([]), 3000);
        });
        if (files.length) startSend(files);
        else { location.hash = '#/'; }
    } catch (e) { location.hash = '#/'; }
};

// ===================== boot =====================
window.addEventListener('unhandledrejection', (e) => {
    console.error('unhandled', e.reason);
});

if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('sw.js').catch(() => {});
}
sweepOPFS();

if (location.hash === '#/shared') takeShared();
else route();
