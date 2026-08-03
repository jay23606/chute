import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
import * as U from './util.js';

// ===================== config =====================
// Supabase is used for ONE thing: relaying the WebRTC handshake (Realtime Broadcast).
// No tables, no auth, no storage — file bytes never touch it. The publishable key is
// public by design.
const SUPABASE_URL = 'https://zbtgonklxweikgukzukg.supabase.co';
const SUPABASE_KEY = 'sb_publishable_Tpkd3FzWhsfldMll-gIqfg_74YVroef';

const sb = createClient(SUPABASE_URL, SUPABASE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
    realtime: { params: { eventsPerSecond: 40 } },   // ICE candidates arrive in bursts
});

// ===================== tiny DOM helpers =====================
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const el = (html) => { const t = document.createElement('template'); t.innerHTML = html.trim(); return t.content.firstElementChild; };
const on = (node, ev, sel, fn) => node.addEventListener(ev, (e) => {
    const t = e.target.closest(sel);
    if (t && node.contains(t)) fn(e, t);
});
const app = () => $('#app');

let toastT = null;
const toast = (msg, ms = 2600) => {
    const n = $('#toast');
    if (!n) return;
    n.textContent = msg;
    n.classList.add('show');
    clearTimeout(toastT);
    toastT = setTimeout(() => n.classList.remove('show'), ms);
};

// Clipboard with a fallback for non-secure / older contexts.
const copy = async (text) => {
    try { await navigator.clipboard.writeText(text); return true; } catch (e) { /* fall through */ }
    try {
        const ta = Object.assign(document.createElement('textarea'), { value: text });
        ta.style.cssText = 'position:fixed;opacity:0';
        document.body.appendChild(ta); ta.select();
        const ok = document.execCommand('copy');
        ta.remove();
        return ok;
    } catch (e) { return false; }
};

// Everything after the '#' is the room code. Deliberately in the fragment: browsers never
// send it to the server, so the share link's secret stays between the two people.
const shareUrl = (code) => `${location.origin}${location.pathname}#${code}`;

export { sb, $, $$, el, on, app, toast, copy, shareUrl, U };
export const { esc, fmtBytes, fmtRate, fmtDur, makeCode, normCode, isCode,
    safeName, dedupeName, pickChunk, makeMeter, throttle, rand, sum, pct } = U;
