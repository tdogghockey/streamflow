# Stream Checking

## Overview

Stream checking is the third step of the automation pipeline. It uses ffmpeg to analyze each stream, scores based on quality dimensions, and reorders streams so the best one is at the top of the channel in Dispatcharr.

---

## Quality checking

Each stream is analyzed by spawning a short ffmpeg probe session. Extracted metrics:

- Bitrate (kbps)
- Resolution (width × height)
- FPS
- Codec (H.264, H.265/HEVC, AV1, etc.)
- HDR (detected from ffmpeg stderr: pixel format, color space, primaries, transfer function)
- Blank-screen status (optional; parsed from ffmpeg `blackdetect` output)
- Error presence (dropped frames, decode errors)

---

## Missing bitrate and recheck

When an initial probe proves that a stream is playable but cannot measure its
current bitrate, StreamFlow does not reuse an older bitrate as the current
measurement. After all initial probes for that channel finish, the affected
streams receive a lightweight bitrate-only recheck one at a time, in stable
stream order, before the next channel starts.

The recheck uses the configured probe timing and existing provider, profile,
and global capacity limits. It does not repeat blank/freeze decoding, and a
failed recheck does not turn the playable stream into a dead stream or erase
the initial visual evidence. If the retry succeeds, the recovered value becomes
the current bitrate. If it still cannot be measured, the current result stays
`N/A` with `Bitrate unavailable after recheck`; any older stored bitrate remains
ranking-only evidence.

This is automatic backend behavior, not a user setting. Current activity is
visible at `Stream Checker -> Current Progress (active run) -> Stream Progress
Tracking -> Status -> Bitrate Recheck`; completed evidence is under `Changelog
-> Action filter: Automation Runs -> Automation Period (expand) -> <channel> ->
Quality Check (expand) -> Analyzed Streams -> Reason`.

---

## Stream cache

Stream checking always persists each stream's measured stats (resolution,
bitrate, codec, FPS, quality score) back to Dispatcharr after analysis. The
**stream cache** reuses those recently-persisted stats instead of re-probing a
stream with ffmpeg when the same stream is picked up again — for example, after
an external reassignment (Teamarr or a manual Dispatcharr edit) reorders a
channel.

Enable it at `Stream Checker -> Stream Checker Configuration -> Edit -> Stream
Analysis -> Reuse Cached Stream Stats`; the corresponding config block is:

```json
{
  "stream_cache": {
    "enabled": false,
    "ttl_hours": 48
  }
}
```

A stream's cached measurement is reused only when it is recent (within
`ttl_hours`), was measured successfully, and is not marked for recheck or dead.
Fresh streams are re-sorted by cached score but not re-probed; new or stale
streams are still fully analyzed. Force checks and the normal re-sort always run
— the cache only skips the ffmpeg probe step.

Default is **off** (opt-in). Cache hits are visible in the backend log as
`Stream cache: reusing N fresh stream(s) … (skipping ffmpeg re-probe); probing M`.

---

## Scoring

Streams are scored 0–100 using weighted dimensions. Configure the weights at
`Settings -> Profiles tab -> Edit profile -> Stream Checking -> Stream Quality
Scoring`; `scoring_weights` is the corresponding profile configuration object.

| Dimension  | Default weight |
| ---------- | -------------- |
| Bitrate    | 35%            |
| Resolution | 30%            |
| FPS        | 15%            |
| Codec      | 10%            |
| HDR        | 10%            |

**M3U source priority** is applied on top of the quality score. Priority values are 0–100 (higher = more preferred). Two modes:

- `absolute` — higher-priority source streams always rank above lower-priority streams regardless of quality score
- `equal` — quality score only, M3U account is ignored for ordering

---

## Filters

Before scoring, streams can be discarded based on minimum thresholds. Configure
resolution, FPS, and bitrate at `Settings -> Profiles tab -> Edit profile ->
Stream Checking -> Minimum Quality Requirements`, and blank detection at the
same Stream Checking step under `Check streams for blank screens`.

| Field            | Effect                                                      |
| ---------------- | ----------------------------------------------------------- |
| `min_resolution` | Skip streams below this resolution                          |
| `min_fps`        | Skip streams below this FPS                                 |
| `min_bitrate`    | Skip streams below this bitrate (kbps)                      |
| `blank_check_enabled` | Mark streams dead when most of the probe window is blank |

Blank detection is folded into the same ffmpeg process as the quality probe.
It adds a second ffmpeg output from the already-open input instead of starting
ffprobe or a second provider connection, so single-stream provider limits are
respected.

---

## Parallel checking

Configure concurrent checking at `Stream Checker -> Stream Checker Configuration
-> Edit -> Concurrent Checking tab`.

Stream checking runs in a thread pool. The pool size is configurable. Distinct
usable credential-route components represent independent provider credentials
and are enforced separately. Profiles that resolve to the same credential
target, including default aliases, share one component whose capacity is the
strictest finite limit among those aliases. Finite component limits are summed
for the account aggregate; if any distinct component is unlimited
(`max_streams: 0`), the aggregate is unlimited. The M3U account `max_streams`
value is used only as a fallback when the account has no active provider profile
credentials. If active profiles exist but none can provide a usable route for a
stream, that check fails closed instead of falling back to the stored URL.

Every probe URL is resolved while reserving its exact profile and remains bound to that reservation. A default profile may use the stored provider URL; every non-default profile must produce its own valid credential rewrite or the probe fails closed. The Profile Matrix is read-only status/API information; profile limits and credential rewrites are not editable there.

Provider capacity is never inferred from malformed or missing authority. A
missing account/profile inventory or an invalid route ends that probe immediately
with `provider_profile_unavailable`; an unavailable or malformed live proxy-status
usage read is `provider_usage_unavailable` and waits safely until the configured
timeout. Neither condition is treated as zero usage. Inspect `Stream Checker ->
Current Progress (active run) -> Stream Progress Tracking -> Status/reason` or
`GET /api/stream-checker/progress -> streams_detail[].reason_detail|skipped_reason`
before changing limits.

The matrix is visible during an active run at `Stream Checker -> Current
Progress (active run) -> Profile Matrix (expand)`; its API source is `GET
/api/stream-checker/progress -> provider_progress[].profile_slots[]`.

Per-stream reservation telemetry is visible at `Stream Checker -> Current
Progress (active run) -> Stream Progress Tracking -> Account` (profile name;
hover for ID and Limit), and at `GET /api/stream-checker/progress ->
streams_detail[].reserved_profile_id|reserved_profile_name|reserved_profile_limit`.
The reported limit is the capacity actually enforced for that reservation; when
profile aliases share one credential route, it is their strict shared-route limit
rather than a looser raw profile value. It contains no probe URL or credentials.
Waiting or viewer-preempted rows clear a released profile; an initial probe and
serial bitrate recheck may therefore show different exact profiles. A completed
live row retains the profile that actually performed its final probe.

Long loop-detection probes use the same account and profile reservations, apply the URL transformation of the profile they actually reserve, and stop without recording a clean result when manual cancellation or real-viewer preemption occurs.

Enable `Check All Streams in Channel` at `Settings -> Profiles tab -> Edit
profile -> Stream Checking` to check every stream on a channel. The profile key
is `check_all_streams`; by default only the currently active (top) stream is
checked.

`Stream Limit per Channel` at `Settings -> Profiles tab -> Edit profile -> Stream
Checking` caps the maximum number of streams checked per channel per run; its
profile key is `stream_limit`.

---

## Dead stream tracking

Streams that fail checking are marked dead in the `DeadStreamsTracker`. Dead streams are:

- Excluded from quality scoring
- Optionally removed from the channel automatically

If `allow_revive: true` is set in the profile, dead streams are re-checked on each run and restored to the channel if they pass.

---

## Connectivity guard

Stream checking includes a fail-closed connectivity guard before destructive
quality-check operations. By default, StreamFlow verifies both general internet
reachability and the configured Dispatcharr API before a quality check can mark
streams dead or write a changed stream list back to a channel.

The guard also re-checks connectivity immediately before dead-stream marking and
channel stream updates. If DNS, internet access, the gateway, or the Dispatcharr
API cannot be verified, the quality-check step aborts and leaves channel stream
assignments unchanged.

The guard can be disabled at `Stream Checker -> Stream Checker Configuration ->
Edit -> Safety tab -> Connectivity Guard` or by setting:

```json
{
  "connectivity_guard": {
    "enabled": false
  }
}
```

---

## Stream protection (hysteresis)

StreamFlow distinguishes between streams that are **currently in use** and streams that are idle. Currently active streams receive a longer grace period before being replaced — even if a higher-scoring stream is available — to avoid interrupting live playback. Idle streams are replaced aggressively.

Enable `Respect 2h Grace Period` at `Settings -> Profiles tab -> Edit profile ->
Stream Checking` to enable the grace window for checked streams; its profile key
is `grace_period`.

---

## Provider live probe (single-connection guard)

For M3U accounts with a **1-connection provider limit** (`max_streams: 1` on an
Xtream-type account), StreamFlow checks the provider's live connection state
before any probe or playlist work touches that account. The goal: when someone
is watching a stream from that provider (directly in their player), automated
checks must not open a competing connection and disrupt live playback.

Enable it at `Stream Checker -> Stream Checker Configuration -> Edit`; the
corresponding config block is:

```json
{
  "provider_live_probe": {
    "enabled": true,
    "cache_ttl": 45,
    "timeout": 10,
    "retry_interval": 60,
    "max_retries": 3
  }
}
```

### How it works

- **Probe endpoint** — `GET {server}/player_api.php?username=…&password=…`
  returns `user_info.active_cons` and `user_info.max_connections` (string-encoded
  ints, nested under `user_info`, not top level). This request never consumes a
  stream slot.
- **HTTP layer** — the probe shells out to `curl` (`User-Agent: VLC/3.0.14`)
  rather than python-requests. Some provider CDNs (Cloudflare) intermittently
  block python-requests' TLS fingerprint with 520/513 errors; curl passes
  consistently. If `curl` is unavailable the probe falls back to requests.
- **Fail closed** — an unavailable, malformed, or unrecognizable response is
  treated as busy (never as a confirmed "0 connections"), so a broken probe
  cannot open the scheduler while a viewer may be present. The account limiter
  still decides admission with its own capacity checks.
- **Mirror accounts** — accounts sharing one username (failover hosts pointing
  at different provider URLs) are grouped: **any mirror free = free**, all
  mirrors busy/unknown = busy. Credentials come from the UDI account data.
- **Shared busy store** — UDI account accessors return deep copies, so
  busy/unbusy state is kept in a module-level store
  (`apps/stream/provider_live_probe.py`) that every check path reads. It is
  re-evaluated on each run's inventory initialization and cleared when a probe
  confirms a free slot.
- **Fresh verdicts** — the per-stream probe gate uses a 10-second TTL (instead
  of the 45s cache) so a viewer starting to watch — or reconnecting after a
  drop — is detected within 10 seconds instead of after a full cache window.
  This prevents the kick/oscillation spiral where a stale "free" verdict lets a
  probe start just as the viewer's connection returns.

### What gets deferred

When the provider reports busy:

- The account's stream probes are **skipped instantly** (reason
  `provider_live_busy`) with any cached stats attached — no
  `provider_wait_timeout` is burned per stream.
- Channel-level pre-checks (`check_single_channel` and `_check_channel_concurrent`)
  consult the shared busy store first and defer the **whole channel** before any
  analysis starts.
- M3U playlist re-downloads for the account are skipped (a full playlist fetch
  competes with the live stream on the same provider edge).
- Scheduled UDI refresh ticks skip while the provider is busy.
- Checks resume automatically once a probe confirms a free slot.

### Where it is enforced

The gate runs on every path that could open a provider connection or trigger a
provider-side fetch:

- `_initialize_provider_probe_account_inventory` (inventory publication)
- `_run_capacity_limited_stream_probes` (all capacity-limited probes)
- `check_single_channel` / `_check_channel_concurrent` (channel pre-checks)
- The limiter's per-stream `check_stream_can_run` gate (fresh 10s verdict
  immediately before each probe)
- The M3U playlist refresh step inside channel checks
- The scheduled UDI refresh worker

### Limits of the design

- **Direct-player connections are only visible via the provider.** Dispatcharr's
  proxy status cannot see streams a player opens directly against the provider,
  so the probe is the only signal. Backing out of a player app does not close
  the connection — fully close the app (force stop) when done watching, or the
  provider keeps counting the slot and checks stay deferred for a few minutes.
- **Stale provider slots** — some providers keep counting a closed connection
  for minutes after a non-graceful disconnect. During that window checks defer
  even though nothing is playing; they self-heal once the probe reports
  `active_cons: 0`.
- The guard only applies to accounts with a real 1-connection cap
  (`max_streams: 1`) on an http(s) provider. Unlimited accounts (`0`) and
  custom/local accounts consume no provider slots and are never probed.

### Observability

- Busy defers: `Account <name> is busy (provider_live_busy), skipping live probes`
  and `Channel <id> account <name> is live-busy, deferring …`
- Skipped streams carry `skipped_reason: provider_live_busy` /
  `defer_reason: provider_live_busy` in check results and progress details.
- Probe failures log `Live probe failed for (<server>, <user>)` at WARNING with
  the exception type; credentials are redacted in all probe logs.
- Free-slot verdicts log at DEBUG only (`Mirror <url> has free slot …`) to avoid
  spamming the wait-loop poll cadence.

---

## Concurrent checking

Concurrent stream probes are handled inside the stream checker service with
account-aware limits. Progress is logged periodically during large runs.
