# chute 🪂

**Send very large files straight from your device to someone else's.** No upload, no size
limit, no account, nothing stored on a server.

Live: **https://jay23606.github.io/chute/**

Drop a file in, get a link, send the link. When they open it the two browsers connect
directly and the bytes flow peer to peer, straight into a file on their disk.

---

## How it works

```
  you                     Supabase Realtime                    them
   │                    (handshake messages only)               │
   ├──── "here's my offer" ──────────────────────────────────►  │
   │  ◄─────────────────────────────────── "here's my answer" ──┤
   │                                                            │
   ╰══════════ encrypted WebRTC data channel ═══════════════════╯
                  every byte of every file goes here
```

Supabase Realtime Broadcast relays the dozen or so messages the two browsers need to find
each other, and nothing else — **no tables, no auth, no storage, no schema to deploy.** The
files themselves never touch it, or any other server.

### Why there's no size limit

Neither side ever holds a whole file in memory:

- **Sending** — chute keeps a `File` handle and reads it a slice at a time with
  `file.slice(offset, offset + chunk)`. A 50 GB file costs the same memory as a 50 MB one.
- **Receiving** — chunks are written straight to disk as they arrive via the File System
  Access API. Firefox and Safari, which lack it, stage the file in the Origin Private File
  System instead and then hand the browser a disk-backed `File` to download — still O(1)
  memory, but it needs roughly the file's size in free space first.

Both directions are flow-controlled, so neither a slow disk nor a slow link can make memory
run away:

- the sender pauses when the data channel's send buffer passes 8 MB (`bufferedamountlow`),
- the receiver tells the sender to `pause` when its pending-write queue passes 24 MB, and
  `resume` once it drains — because a fast network with a slow disk is the normal case.

Chunk size is negotiated, not guessed: chute asks the transport for `sctp.maxMessageSize`
and uses up to 256 KB where it's available, falling back to 16 KB where it isn't.

## Resuming after a dropped connection

Wi-Fi blips, laptops sleep, phones change cell. On a transfer measured in hours, starting a
20 GB file over from zero isn't an option — so chute doesn't.

When the connection dies mid-file the receiver **keeps the part-written file and its open
sink**, and goes into a `reconnecting` state instead of throwing anything away. It also starts
re-announcing itself on the signaling channel, so as soon as the sender is reachable again the
two re-dial, and the receiver answers the sender's next `{t:'file'}` with the byte it stopped
on:

```
recv → { t:'ready', id, offset: 8_142_483_456 }
```

The sender seeks its `File` to that offset and carries on. **The receiver is the sole authority
on where to resume** — it's the side that knows what actually reached the disk — so the sender
needs no memory of the interrupted attempt at all. Files already finished aren't re-offered,
and the progress bar picks up where it left off rather than restarting.

The offset is remote input used as a loop bound, so it's clamped into the file
(`clampOffset`, unit-tested): a negative value would spin forever and an oversized one would
silently skip data.

If the sender never comes back, **Stop waiting and keep what arrived** commits the partial
file rather than binning it.

**What this doesn't cover:** closing or crashing the receiving tab. The File System Access
API only commits a writable stream on `close()`, so there's no durable partial to resume from,
and periodically committing to get one would mean re-copying the whole file each time
(`keepExistingData` is a real copy) — O(n²) on exactly the huge files that need it most. Doing
that properly means writing sequential part-files and merging at the end; it's not built.

## How private is it?

The data channel is encrypted end to end (DTLS) and the file bytes only ever exist on the
two devices involved.

**The room code is the key.** Anyone who has it can receive the files, so share it the way
you'd share a password. It lives in the part of the link after the `#`, which browsers never
send to a server — so the link's secret doesn't end up in anyone's HTTP logs.

Once connected, both screens show the same **six-digit safety number**, derived from both
sides' DTLS fingerprints. If the numbers match, nobody is sitting in the middle. If they
don't, stop.

## Things to know

- **The sender's tab has to stay open** for the whole transfer. The files are coming from
  that device — there's no copy anywhere else.
- **Some mobile networks** block direct connections between devices (symmetric / carrier-grade
  NAT). On Wi-Fi it almost always works. To fix it everywhere, put TURN credentials in the
  `TURN` array at the top of [`rtc.js`](rtc.js).
- **Dropped connections resume automatically** (see below) — but only while both tabs stay
  open. Closing the receiving tab discards a part-received file.

## Install it

It's a PWA: installable from the browser's install button, works offline (the UI, not the
transfers, obviously), and registers as an **OS share target** on Android — so "Share → chute"
from your gallery or file manager drops you straight into a ready-to-send link.

## The code

No build step. Native ES modules and a plain stylesheet, served as-is by GitHub Pages.

| file | what's in it |
| --- | --- |
| [`util.js`](util.js) | pure helpers — formatting, code generation, filename sanitising, chunk sizing, rate meter. No DOM, no network, fully unit-tested. |
| [`core.js`](core.js) | config, the Supabase client, tiny DOM helpers |
| [`rtc.js`](rtc.js) | WebRTC over Supabase Broadcast: rooms, offer/answer/ICE, the safety number |
| [`sink.js`](sink.js) | where received bytes go — the four-tier File System Access → OPFS → memory ladder |
| [`transfer.js`](transfer.js) | the wire protocol, chunking, and both directions of flow control |
| [`app.js`](app.js) | views, router, drag & drop, install prompt, share target |
| [`sw.js`](sw.js) | offline shell cache + share-target handling |

### The protocol

One reliable, ordered data channel per peer carries both control messages (JSON strings) and
file bytes (raw `ArrayBuffer`s). Because ordering holds across both, "here comes file X" →
chunks → "that was all of X" needs no framing of its own.

```
sender → { t:'manifest', files:[…] }        what's on offer
recv   → { t:'start', ids:[…] }             a destination was picked, go
sender → { t:'file', id, name, size }       next file
recv   → { t:'ready', id, offset } | { t:'skip' }
                                            the sink is open (no race with the first chunk),
                                            and `offset` is where to resume from
sender → «binary chunks from offset…» → { t:'end', id }
recv   → { t:'ok', id } | { t:'fail', id }  flushed to disk
recv   → { t:'pause' } / { t:'resume' }     receiver-side flow control, any time
sender → { t:'done' }
```

A sender can serve several receivers at once — each gets its own send loop over the same
`File` handles. Files added mid-session are pushed as an updated manifest, and a receiver
that already chose a destination picks them up automatically — the same path a reconnecting
receiver uses to ask for everything still outstanding.

### Tests

```bash
npm test
```

`node --test`, zero dependencies. Covers the sanitiser against path-traversal filenames, the
resume-offset clamp, the rate meter's convergence and ETA, chunk-size negotiation, the code
alphabet, and the progress throttle's leading/trailing behaviour.

Beyond the unit tests, resume is verified in two real browsers: a 400 MB transfer severed
three times mid-file (by closing the `RTCPeerConnection` outright) reassembles **byte-for-byte
identical**, and a multi-file batch interrupted after the first file completes doesn't re-send
what already landed.

## Family

Built from the same engine as [peek](https://github.com/jay23606/peek),
[mayfly](https://github.com/jay23606/mayfly), and
[slumegle](https://github.com/jay23606/slumegle) — raw WebRTC signalled over Supabase
Realtime Broadcast, no PeerJS cloud, no build step.
