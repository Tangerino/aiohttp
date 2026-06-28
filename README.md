# aiohttp functional test

`test_aiohttp.py` exercises the MicroPython [`aiohttp`](aiohttp.py) client against
real HTTP/WebSocket endpoints. It runs on **a board (e.g. ESP32) over WiFi** —
the source of truth — and on **a PC under CPython** for fast iteration (see
[Two runtimes](#two-runtimes)).

A remote host that 5xx's or is unreachable after retries, or an endpoint a host
does not provide, is reported as **SKIP** (not FAIL) — those are service issues,
not client bugs.

## What it tests

Each check prints a `PASS`/`FAIL` line; tests may `SKIP`; the run ends with a
`summary` line counting passed / failed / skipped.

| Test            | Checks                                                |
|-----------------|-------------------------------------------------------|
| `get_status`    | `GET /get` returns 200 and echoes the URL             |
| `get_params`    | query params are reflected back                       |
| `text_json`     | `resp.text()` / `resp.json()` readers (Content-Length path) |
| `post_json`     | JSON body round-trips                                  |
| `post_data`     | raw text body round-trips                              |
| `post_bytes`    | `bytes` body → `application/octet-stream`, round-trips |
| `custom_header` | a custom request header is sent                       |
| `methods`       | GET/PUT/PATCH/DELETE all return 200                   |
| `head_options`  | HEAD (empty body) and OPTIONS                          |
| `redirect`      | absolute redirect followed (http→https by default; override/blank via `REDIRECT_URL`) |
| `compression`   | gzip response transparently inflated to valid JSON    |
| `deflate`       | deflate response inflated (uses `DEFLATE_URL`; SKIPs only if that host is down) |
| `websocket`     | text echo (binary/JSON best-effort; text-only servers WARN) |
| `ws_iter`       | `async for msg in ws` iteration + `WSMsgType`         |
| `speed`         | single download streamed in fixed-size chunks + throughput |
| `timeout_connect`  | a never-completing connect aborts at the session timeout |
| `timeout_read`     | a stalled response aborts at the read timeout (SKIPs if host won't delay) |
| `timeout_generous` | a generous timeout does **not** disturb a normal request |
| `timeout_override` | per-request `timeout=` overrides the session default  |

## Timeouts

The client supports a per-operation timeout (seconds) — essential when polling
HTTP devices on a LAN, where some are offline (connect hangs on an unanswered
SYN) or wedged (the response never arrives). It is applied to **every** network
wait: connect, send, and each read. A single operation that stalls past the
timeout aborts the request with `asyncio.TimeoutError`, and the socket is dropped
so a later request can't inherit a half-spoken connection.

```python
# Default for every request on the session (None = wait forever, the default):
async with aiohttp.ClientSession(timeout=2.0) as s:
    async with s.get("http://192.168.1.50/status") as r:   # offline -> aborts in ~2s
        data = await r.json()

# Override per request (None disables it for that call only):
async with s.get("http://192.168.1.50/slow", timeout=0.5) as r:
    ...
```

It is built on `asyncio.wait_for`, which exists on both MicroPython (≥1.13) and
CPython, so the same code times out identically on a board and on a PC.

## One file

The client is a **single module, [`aiohttp.py`](aiohttp.py)** — the upstream
package's two modules (`__init__.py` + `aiohttp_ws.py`) unified into one file, so
there is no duplicate copy to keep in sync and it deploys with **no subpackage
folder** (handy under OTA constraints). HTTP **and** WebSocket are included.
Upstream ships this as a package, so this copy diverges.

## Credits

This module is **derived from the [`aiohttp`](https://github.com/micropython/micropython-lib/tree/master/python-ecosys/aiohttp)
package in [micropython-lib](https://github.com/micropython/micropython-lib)**,
originally written by **Carlos Gil** (MIT licensed), which is in turn adapted from
[danni/uwebsockets](https://github.com/danni/uwebsockets) and the WebSocket code
in [miguelgrinberg/microdot](https://github.com/miguelgrinberg/microdot). The
single-module port plus the CPython, timeout, and keep-alive work in this copy are
by Carlos Tangerino. All original copyright notices are retained in `aiohttp.py`,
and this project remains under the MIT license. Thanks to the upstream authors.

## Differences from upstream

This copy is a superset of upstream's behaviour; existing call sites keep working.
What changed:

| Area | Upstream (micropython-lib) | This module |
|------|----------------------------|-------------|
| Packaging | `aiohttp/` package, two modules | single `aiohttp.py` |
| Runtime | MicroPython only | MicroPython **and** CPython (stdlib-only) |
| Timeouts | `# TODO: Implement timeouts` | per-operation timeout (connect/send/read) via `asyncio.wait_for`, session default + per-request override |
| Connection | HTTP/1.0, `Connection: close`, one socket per request | HTTP/1.1 **keep-alive**: one TCP/TLS socket reused per host, with stale-socket detection + one transparent reconnect |
| Body tracking | none | tracks `Content-Length` / `_read` so keep-alive knows when the socket is drained; HEAD and 204/304 pinned to empty body |
| gzip/deflate | MicroPython `deflate` module only (prints a warning if missing) | falls back to stdlib `zlib` on CPython |
| Header parsing | case-**sensitive** match on `Transfer-Encoding:` / `Location:` | case-**insensitive** (handles lowercased headers from go-httpbin / HTTP/2 proxies) |
| Socket teardown | `reader.aclose()` (MicroPython API) | `_aclose()` helper: `reader.aclose()` on MicroPython, writer + `transport.abort()` on CPython |
| Request bytes | bytes-`%` formatting + `writer.awrite()` (MicroPython-only) | build as `str` then `.encode()`, `writer.write()` + `drain()` (works on both) |
| Default version | `HttpVersion10` | `HttpVersion11` |

The HTTP/WebSocket framing, the public `ClientSession` / `ClientResponse` /
`ClientWebSocketResponse` API, and the redirect handling are otherwise unchanged
from upstream.

## Two runtimes

This module and its test run on **both MicroPython and CPython**. The few APIs
that differ are handled inline with the idiomatic `try/except ImportError`
pattern — gzip via the `deflate` module falling back to stdlib `zlib`, connection
teardown via `reader.aclose()` (MicroPython) or the writer (CPython), and a
`ticks_ms`/`ticks_diff` shim in the test. The same code runs on a board and on
your PC using only each runtime's standard library — no pip packages, no extra
modules.

- **On a board (source of truth)** — `mpremote`, see below. Validates real
  sockets/TLS over WiFi.
- **On your PC (fast iteration / CI)** — plain CPython, no hardware:

  ```bash
  cd aiohttp
  ../.venv/bin/python test_aiohttp.py        # hits real network
  ```

  Defaults target **postman-echo.com** (reliable) plus a **Cloudflare** sized
  download for the speed test; WiFi bring-up is skipped automatically off-device.
  The echo-shape assertions are host-agnostic — they accept postman-echo, httpbin,
  and go-httpbin shapes (go-httpbin wraps echoed args/headers in single-element
  lists and base64-wraps untyped bodies). A flaky host's 5xx/unreachable responses
  become `SKIP`, so a green-but-skipped run still means the client is fine.

## Files the test needs on the device

| File              | Why                                                |
|-------------------|----------------------------------------------------|
| `aiohttp.py`      | the client under test (single module)              |
| `test_aiohttp.py` | the test runner (all config inline)                |

## 1. Configure

All configuration lives in the **`CONFIG` block at the top of `test_aiohttp.py`**
— edit the constants directly. Set `WIFI_SSID` / `WIFI_PASSWORD` for on-device
runs, and point `HTTP_BASE` / `WS_URL` (and `SPEED_URL`, `REDIRECT_URL`,
`DEFLATE_URL`) at your own server if you don't want the defaults. On a PC the host
network is used and WiFi is skipped, so the WiFi values are ignored there.

A few endpoints use a host other than `HTTP_BASE` because postman-echo doesn't
provide them: `REDIRECT_URL` defaults to `http://github.com` (a single
http→https redirect landing on 200), and `DEFLATE_URL` defaults to
`https://httpbingo.org/deflate` (postman-echo's `/deflate` answers 200 *without*
actually deflating, which would force a SKIP). Blank either to skip that test.

## 2. Run on the board

Run all commands from this folder (`aiohttp/`). Find your port with
`mpremote devs` and pass it via `connect PORT`. **Always pin the port** — when
more than one board is attached, the short form cannot guess which one you mean.
Examples below use `/dev/tty.usbserial-1330`; substitute your own.

### Option A — mount and run (recommended, no copying)

Exposes this folder to the board for the duration of the run; nothing is written
to flash, so you can edit and re-run instantly.

```bash
mpremote connect /dev/tty.usbserial-1330 mount . run test_aiohttp.py
```

### Option B — copy to flash (persistent)

```bash
mpremote connect /dev/tty.usbserial-1330 \
    cp aiohttp.py : + cp test_aiohttp.py :
mpremote connect /dev/tty.usbserial-1330 run test_aiohttp.py
```

The board's root is on the default `sys.path`, so `import aiohttp` finds
`aiohttp.py`. **Make sure no package form shadows it** — if a previous deploy
left `:lib/aiohttp/`, remove it first (`mpremote connect PORT rm -r :lib/aiohttp`),
since a package directory takes precedence over a same-named `.py` module.

### Alternative — install with `mip`

```bash
mpremote connect /dev/tty.usbserial-1330 mip install aiohttp
```

Note `mip install aiohttp` pulls the **upstream package** form (a folder), not
this single-module copy with its dual-runtime / case-insensitive-header changes.
You still need to copy `test_aiohttp.py` to run the suite.

## Expected output

Logging is verbose: every line is timestamped (ms since start), every step is
logged, board/runtime resources are printed up front, and each `TEST`/`DONE` line
shows progress (`[current/total %]`). On a board `DONE` also reports free heap, so
memory leaks are easy to spot.

```
[       3 ms] INFO  starting aiohttp functional tests (19 tests)
[      14 ms] INFO  machine:  Generic ESP32 module with SPIRAM with ESP32
[      15 ms] INFO  firmware: esp32 1.26.0
[      31 ms] INFO  heap:     4152128 free / 4184768 total bytes
[     172 ms] INFO  WiFi connected: ('192.168.0.103', ...)
[     173 ms] INFO  HTTP base: https://postman-echo.com
[     178 ms] INFO  WS url:    wss://ws.postman-echo.com/raw
[     189 ms] TEST  [1/19   5%] get_status
[     191 ms]   STEP  requesting /get
[     193 ms]     STEP  -> GET https://postman-echo.com/get
[    2054 ms]     STEP  <- HTTP 200 in 1862 ms, 165 bytes
[    2056 ms]   PASS  GET /get returns 200 (got 200)
[    2061 ms]   PASS  GET /get echoes url
[    2077 ms] DONE  [1/19   5%] get_status (1873 ms, 2 passed / 0 failed, 4146512 B heap free)
...
[   14655 ms]   PASS  absolute redirect followed to 200 (got 200)
...
[   27833 ms] INFO  summary: 39 passed, 0 failed, 0 skipped of 39 checks in NNNN ms (NN.N s)
```

A non-zero `failed` count means a real check failed (prefixed `FAIL`/`ERROR`).
`SKIP` lines are service/endpoint gaps, not client bugs.

## Troubleshooting

- **`could not enter raw repl` / port busy** — close any open REPL or serial
  monitor, replug the board, or recheck the `PORT` from `mpremote devs`.
- **`WiFi connection failed`** — wrong `WIFI_SSID`/`WIFI_PASSWORD`, or out of
  range. ESP32 only joins 2.4 GHz networks.
- **`ImportError: no module named 'aiohttp'`** — `aiohttp.py` is not on the
  device `sys.path`. Use Option A (mount) or copy it to the board root (Option B).
- **`OSError` / TLS errors on `https`/`wss`** — some boards need more heap or
  current firmware for TLS. Point `HTTP_BASE`/`WS_URL` at plain
  `http`/`ws` endpoints if needed.
- **Lots of `SKIP` lines** — the configured host is overloaded (5xx) or
  unreachable; the client is fine. Re-run, or point the endpoints at a healthier
  host / your own server. (httpbin.org in particular is often rate-limited.)
