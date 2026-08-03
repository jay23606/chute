// Pure, dependency-free helpers. No DOM, no network — so they're unit-testable in Node
// (see test/util.test.js). Everything else in the app imports these via core.js.

const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// ---- formatting ----
const UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
const fmtBytes = (n) => {
    n = Number(n) || 0;
    let i = 0;
    while (n >= 1024 && i < UNITS.length - 1) { n /= 1024; i++; }
    return `${n < 10 && i > 0 ? n.toFixed(1) : Math.round(n)} ${UNITS[i]}`;
};
const fmtRate = (bytesPerSec) => (bytesPerSec > 0 && isFinite(bytesPerSec)) ? `${fmtBytes(bytesPerSec)}/s` : '—';
const fmtDur = (secs) => {
    if (!isFinite(secs) || secs < 0) return '—';
    secs = Math.round(secs);
    if (secs < 60) return `${secs}s`;
    if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
    return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
};

// ---- room codes ----
// Crockford-ish alphabet: no 0/O/1/l/i/u — the code gets read aloud and typed by hand.
const ALPHABET = '23456789abcdefghjkmnpqrstvwxyz';
// 8 chars of a 30-letter alphabet ≈ 39 bits. The code IS the capability to receive the
// files (see README "How private is it"), so it has to be unguessable, not just unique.
const makeCode = (len = 8, bytes) => {
    const b = bytes || ((typeof crypto !== 'undefined' && crypto.getRandomValues)
        ? crypto.getRandomValues(new Uint8Array(len))
        : Uint8Array.from({ length: len }, () => Math.floor(Math.random() * 256)));
    let out = '';
    for (let i = 0; i < len; i++) out += ALPHABET[b[i] % ALPHABET.length];
    return out;
};
// Accept what people actually paste: spaces, dashes, caps, a full share URL.
const normCode = (raw) => {
    const s = String(raw || '').trim();
    const tail = s.includes('#') ? s.slice(s.lastIndexOf('#') + 1) : s;
    return tail.toLowerCase().replace(/[^a-z0-9]/g, '');
};
const isCode = (c) => /^[a-z0-9]{4,32}$/.test(c);

// ---- filenames ----
// The name comes from the *peer*, and the receiver turns it into a real file on disk
// (getFileHandle / <a download>). Strip anything that could escape the chosen folder or
// confuse the OS. The FS Access API rejects separators too, but don't rely on that alone.
const RESERVED = /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\.|$)/i;
const safeName = (name) => {
    let n = String(name || '')
        // eslint-disable-next-line no-control-regex
        .replace(/[\x00-\x1f\x7f]/g, '')                        // no control chars
        // Flatten any path into one name, dropping '.' / '..' segments outright so a
        // relative-path filename can't climb out of the folder the user picked.
        .split(/[\\/]+/).filter((s) => s && s !== '.' && s !== '..').join('_')
        .replace(/^[.\s]+/, '')                                 // no leading dot (hidden file) or space
        .replace(/[\s.]+$/, '')                                 // Windows strips these anyway
        .replace(/[<>:"|?*]/g, '_');                            // illegal on Windows
    if (RESERVED.test(n)) n = '_' + n;
    if (n.length > 180) {                                       // keep the extension when truncating
        const dot = n.lastIndexOf('.');
        const ext = dot > 0 && n.length - dot <= 12 ? n.slice(dot) : '';
        n = n.slice(0, 180 - ext.length) + ext;
    }
    return n || 'file';
};
// Two files named the same in one folder: file.txt → file (2).txt
const dedupeName = (name, taken) => {
    if (!taken.has(name)) return name;
    const dot = name.lastIndexOf('.');
    const base = dot > 0 ? name.slice(0, dot) : name;
    const ext = dot > 0 ? name.slice(dot) : '';
    let i = 2;
    while (taken.has(`${base} (${i})${ext}`)) i++;
    return `${base} (${i})${ext}`;
};

// ---- chunk sizing ----
// SCTP caps a single data-channel message. Chrome/Firefox negotiate 256KB+, older or
// cross-browser pairings can land at 64KB, and Firefox<57 at 16KB. Ask the transport
// rather than guessing: bigger chunks mean far fewer round trips on multi-GB files.
const MIN_CHUNK = 16 * 1024, MAX_CHUNK = 256 * 1024;
const pickChunk = (maxMessageSize) => {
    const max = Number(maxMessageSize);
    if (!isFinite(max) || max <= 0) return MIN_CHUNK;            // 0 / Infinity = "unknown"
    return Math.max(MIN_CHUNK, Math.min(MAX_CHUNK, max - 1024)); // headroom for SCTP framing
};

// ---- resume ----
// Where to restart a file, given the offset the *other side* claims to have. It's remote
// input on a loop bound, so a negative or NaN value would spin forever and an oversized one
// would silently skip data — clamp it into the file rather than trusting it.
const clampOffset = (offset, size) => {
    const n = Number(offset);
    if (!isFinite(n) || n <= 0) return 0;
    return Math.min(Math.floor(n), Math.max(0, Number(size) || 0));
};

// ---- transfer rate meter ----
// Exponentially-weighted so the readout doesn't jump around on a bursty link, but still
// reacts within a few seconds when the real throughput changes.
const makeMeter = (halfLifeMs = 3000) => {
    let lastBytes = 0, lastT = 0, rate = 0;
    return {
        reset(t = 0) { lastBytes = 0; lastT = t; rate = 0; },
        // Feed cumulative bytes + a timestamp; returns bytes/sec.
        push(bytes, t) {
            if (!lastT) { lastT = t; lastBytes = bytes; return rate; }
            const dt = t - lastT;
            if (dt < 250) return rate;                            // too short a window to be meaningful
            const inst = (bytes - lastBytes) * 1000 / dt;
            const a = 1 - Math.pow(0.5, dt / halfLifeMs);
            rate = rate > 0 ? rate + a * (inst - rate) : inst;
            lastBytes = bytes; lastT = t;
            return rate;
        },
        get rate() { return rate; },
        eta(remainingBytes) { return rate > 0 ? remainingBytes / rate : Infinity; },
    };
};

// Progress fires thousands of times a second on a fast link; the UI only needs ~10fps.
// Leading + trailing edge, so the first update is instant and the last one is never lost.
const throttle = (fn, ms = 100, now = () => Date.now(), timer = setTimeout) => {
    let last = -Infinity, t = null, pending = null;   // -Infinity so the very first call is immediate
    return (...args) => {
        pending = args;
        const wait = ms - (now() - last);
        if (wait <= 0) { last = now(); t = null; fn(...pending); return; }
        if (t) return;
        t = timer(() => { last = now(); t = null; fn(...pending); }, wait);
    };
};

const rand = () => Math.random().toString(36).slice(2, 10);
const sum = (arr, key) => arr.reduce((a, x) => a + (Number(key ? x[key] : x) || 0), 0);
const pct = (a, b) => (b > 0 ? Math.min(100, Math.max(0, (a / b) * 100)) : 0);

export { esc, fmtBytes, fmtRate, fmtDur, makeCode, normCode, isCode, safeName, dedupeName,
    pickChunk, clampOffset, makeMeter, throttle, rand, sum, pct, ALPHABET, MIN_CHUNK, MAX_CHUNK };
