import { safeName, dedupeName, fmtBytes } from './util.js';

// ===================== where received bytes go =====================
// The whole point of this app is files too big to hold in RAM, so the receiving side never
// accumulates a multi-GB Blob if it can help it. Three tiers, best first:
//
//   1. "folder"  — File System Access directory handle. One picker for the whole batch,
//                  every file streamed straight to disk. O(1) memory, no size ceiling.
//   2. "file"    — File System Access save picker. Same streaming, one file per prompt.
//   3. "auto"    — no FS Access API (Firefox / Safari): stream into the Origin Private
//                  File System, then hand the browser a disk-backed File to download.
//                  Still O(1) memory; costs a temporary copy in origin storage.
//   4. "memory"  — last resort (no OPFS either): chunks in an array → Blob → download.
//
// Every tier exposes the same `open(name, size) → sink` / `sink.write/close/abort` shape,
// so transfer.js doesn't care which one it got.

const hasFSA = () => typeof window !== 'undefined' && typeof window.showSaveFilePicker === 'function';
const hasDirPicker = () => typeof window !== 'undefined' && typeof window.showDirectoryPicker === 'function';
const hasOPFS = () => !!(navigator.storage && navigator.storage.getDirectory);

const OPFS_DIR = 'incoming';

// Kick off a browser download for a Blob/File we already hold.
const download = (blob, name) => {
    const url = URL.createObjectURL(blob);
    const a = Object.assign(document.createElement('a'), { href: url, download: name, rel: 'noopener' });
    document.body.appendChild(a);
    a.click();
    a.remove();
    // The download reads from the blob lazily — don't yank the URL out from under it.
    setTimeout(() => URL.revokeObjectURL(url), 5 * 60 * 1000);
};

const opfsRoot = async () => (await navigator.storage.getDirectory()).getDirectoryHandle(OPFS_DIR, { create: true });

// A staging file that was written but never cleaned up (tab closed mid-download) would sit
// in origin storage forever. Sweep anything older than an hour at boot.
const sweepOPFS = async () => {
    if (!hasOPFS()) return;
    try {
        const dir = await opfsRoot();
        const cutoff = Date.now() - 60 * 60 * 1000;
        for await (const [name] of dir.entries()) {
            const stamp = Number(name.split('-')[0]);
            if (isFinite(stamp) && stamp < cutoff) await dir.removeEntry(name).catch(() => {});
        }
    } catch (e) {}
};

// ---- individual sinks ----

// Straight to a file the user picked — nothing to do at the end but close it.
const diskSink = (writable, name) => ({
    name,
    async write(buf) { await writable.write(buf); },
    async close() { await writable.close(); return { saved: true, downloaded: false }; },
    async abort() { try { await writable.abort(); } catch (e) {} },
});

const memorySink = (name) => {
    const parts = [];
    return {
        name,
        async write(buf) { parts.push(buf); },
        async close() { download(new Blob(parts), name); parts.length = 0; return { saved: true, downloaded: true }; },
        async abort() { parts.length = 0; },
    };
};

const opfsSink = async (name) => {
    const dir = await opfsRoot();
    const key = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}-${name}`;
    const handle = await dir.getFileHandle(key, { create: true });
    const writable = await handle.createWritable();
    const cleanup = () => dir.removeEntry(key).catch(() => {});
    return {
        name,
        async write(buf) { await writable.write(buf); },
        async close() {
            await writable.close();
            const file = await handle.getFile();
            download(file, name);                             // disk-backed File — never loaded into RAM
            setTimeout(cleanup, 5 * 60 * 1000);               // give the download time to read it
            return { saved: true, downloaded: true };
        },
        async abort() { try { await writable.abort(); } catch (e) {} cleanup(); },
    };
};

// ---- destinations ----

// MUST be called from a user gesture (the pickers require transient activation).
// `fileCount` decides between a one-shot save prompt and a single folder prompt.
const chooseDestination = async ({ fileCount = 1, suggestedName = '' } = {}) => {
    if (fileCount > 1 && hasDirPicker()) {
        const handle = await window.showDirectoryPicker({ id: 'chute', mode: 'readwrite', startIn: 'downloads' });
        if (handle.requestPermission) {
            const p = await handle.requestPermission({ mode: 'readwrite' });
            if (p !== 'granted') throw new DOMException('Permission denied', 'NotAllowedError');
        }
        const taken = new Set();
        return {
            kind: 'folder',
            label: `folder “${handle.name}”`,
            async open(name) {
                const n = dedupeName(safeName(name), taken);
                taken.add(n);
                const fh = await handle.getFileHandle(n, { create: true });
                return diskSink(await fh.createWritable(), n);
            },
        };
    }
    if (fileCount === 1 && hasFSA()) {
        const handle = await window.showSaveFilePicker({ id: 'chute', suggestedName: safeName(suggestedName) || 'download' });
        let used = false;
        return {
            kind: 'file',
            label: `“${handle.name}”`,
            async open(name) {
                if (used) return autoDestination().open(name);   // extra files arrived after the prompt
                used = true;
                return diskSink(await handle.createWritable(), handle.name);
            },
        };
    }
    return autoDestination();
};

// No picker available (or the user is on Firefox/Safari): stage to disk, then download.
const autoDestination = () => ({
    kind: hasOPFS() ? 'auto' : 'memory',
    label: 'your Downloads folder',
    async open(name) {
        const n = safeName(name);
        if (hasOPFS()) {
            try { return await opfsSink(n); } catch (e) { /* private mode / quota → memory */ }
        }
        return memorySink(n);
    },
});

// What to tell the user before they commit to receiving `totalBytes`.
const destinationHint = (fileCount, totalBytes) => {
    if (fileCount > 1 && hasDirPicker()) return 'Pick a folder — every file streams straight to disk.';
    if (fileCount === 1 && hasFSA()) return 'Choose where to save — it streams straight to disk.';
    if (hasOPFS()) return `Saves to your Downloads folder. Needs about ${fmtBytes(totalBytes)} of temporary space first.`;
    return `Heads up: this browser has to buffer the whole ${fmtBytes(totalBytes)} in memory. Chrome or Edge streams it to disk instead.`;
};

// True when we can stream to disk without a same-size temporary copy — used to warn on
// genuinely huge transfers in browsers that can't.
const canStreamToDisk = () => hasFSA() || hasDirPicker();

export { chooseDestination, autoDestination, destinationHint, canStreamToDisk, sweepOPFS, download, hasFSA, hasDirPicker, hasOPFS };
