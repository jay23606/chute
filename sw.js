// Service worker for chute. Three jobs:
//   1. keep the app shell available offline (so it opens instantly / as an installed app),
//   2. handle the OS share sheet ("Share → chute") by stashing the files for the page,
//   3. nothing else — it never sees a byte of an actual transfer, which is peer-to-peer.
const VERSION = 'chute-v2';
const SHELL = [
    './', './index.html', './styles.css', './app.js', './core.js', './util.js',
    './rtc.js', './sink.js', './transfer.js', './icon.svg', './manifest.json',
];

self.addEventListener('install', (e) => e.waitUntil(
    caches.open(VERSION).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting())
));

self.addEventListener('activate', (e) => e.waitUntil(
    caches.keys()
        .then((keys) => Promise.all(keys.filter((k) => k.startsWith('chute-') && k !== VERSION).map((k) => caches.delete(k))))
        .then(() => self.clients.claim())
));

// ---- share target ----
// Files arrive as a POST that no server would answer (GitHub Pages would 404 it), so the
// worker handles it entirely: park the files, then redirect to the app.
const idb = (() => {
    const dbp = new Promise((res, rej) => {
        const r = indexedDB.open('chute-share', 1);
        r.onupgradeneeded = () => r.result.createObjectStore('files', { autoIncrement: true });
        r.onsuccess = () => res(r.result);
        r.onerror = () => rej(r.error);
    });
    const run = async (mode, fn) => {
        const db = await dbp;
        return new Promise((res, rej) => {
            const tx = db.transaction('files', mode);
            const rq = fn(tx.objectStore('files'));
            tx.oncomplete = () => res(rq && rq.result);
            tx.onerror = () => rej(tx.error);
        });
    };
    return {
        add: (v) => run('readwrite', (s) => s.add(v)),
        all: () => run('readonly', (s) => s.getAll()),
        clear: () => run('readwrite', (s) => s.clear()),
    };
})();

const handleShare = async (request) => {
    try {
        const form = await request.formData();
        for (const f of form.getAll('files')) if (f && f.size) await idb.add(f);
    } catch (e) { /* nothing usable was shared — still land the user in the app */ }
    return Response.redirect(new URL('./#/shared', self.registration.scope).href, 303);
};

self.addEventListener('message', (e) => {
    const d = e.data || {};
    if (d.t !== 'take-shared') return;
    const port = e.ports && e.ports[0];
    e.waitUntil((async () => {
        let files = [];
        try { files = await idb.all(); await idb.clear(); } catch (err) {}
        port && port.postMessage({ files });
    })());
});

// ---- shell cache ----
// Network-first: the app is a single page whose whole job is talking to a live peer, so a
// stale build is worse than a slow one. The cache is the offline/flaky-network fallback.
self.addEventListener('fetch', (e) => {
    const req = e.request;
    const url = new URL(req.url);

    if (req.method === 'POST' && url.pathname.endsWith('/share')) {
        e.respondWith(handleShare(req));
        return;
    }
    if (req.method !== 'GET' || url.origin !== self.location.origin) return;

    e.respondWith((async () => {
        try {
            const res = await fetch(req);
            if (res && res.ok && res.type === 'basic') {
                const copy = res.clone();
                caches.open(VERSION).then((c) => c.put(req, copy)).catch(() => {});
            }
            return res;
        } catch (err) {
            const hit = await caches.match(req, { ignoreSearch: true });
            if (hit) return hit;
            if (req.mode === 'navigate') {
                const shell = await caches.match('./index.html');
                if (shell) return shell;
            }
            throw err;
        }
    })());
});
