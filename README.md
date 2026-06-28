# aiohttp

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.x](https://img.shields.io/badge/python-3.x-blue.svg)](https://www.python.org/downloads/)
[![MicroPython 1.18+](https://img.shields.io/badge/micropython-1.18+-blue.svg)](https://micropython.org/)

**An async HTTP + WebSocket client that runs unchanged on CPython and MicroPython — one small file, no dependencies.**

A familiar `aiohttp`-style API (`ClientSession`, `async with session.get(...) as resp`,
`ws_connect`) packed into a **single [`aiohttp.py`](aiohttp.py)** you can drop onto an
ESP32 — or import on your laptop. The same code that polls a sensor in the field is the
code you debug on your PC, against real `https://`/`wss://` endpoints, with no hardware
in the loop.

## Why this module

- **One codebase, two runtimes.** Pure Python, zero runtime dependencies, validated on
  CPython and MicroPython. No conditional branches in *your* code — the runtime
  differences are handled inside the module with the idiomatic `try/except ImportError`
  pattern.
- **Test on PC, deploy to the board.** Debug request/response, redirects, timeouts and
  WebSocket framing on CPython where the tooling is rich, then copy one file to the
  device and run the *same* code over real WiFi.
- **Single file, no subpackage.** Upstream ships a package (`aiohttp/__init__.py` +
  `aiohttp_ws.py`); this is the two unified into one `aiohttp.py` — nothing to keep in
  sync, and a one-line deploy (handy under OTA constraints).
- **Built for long-running devices.** Per-operation timeouts (so a wedged LAN device
  can't hang you forever) and HTTP/1.1 keep-alive (reuse one TCP/TLS socket across
  requests — the per-request TLS handshake dominates on ESP32).
- **HTTP *and* WebSocket** in the same module, behind one `ClientSession`.

## Hello, HTTP

```python
import asyncio
import aiohttp

async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://postman-echo.com/get") as resp:
            print(resp.status)            # 200
            print(await resp.json())      # parsed dict

        # POST JSON, with a 5-second timeout for every network wait:
        async with session.post(
            "https://postman-echo.com/post", json={"hello": "world"}, timeout=5
        ) as resp:
            print(await resp.json())

asyncio.run(main())
```

WebSocket, same session:

```python
async with aiohttp.ClientSession() as session:
    async with session.ws_connect("wss://ws.postman-echo.com/raw") as ws:
        await ws.send_str("ping")
        print(await ws.receive_str())     # 'ping'

        await ws.send_json({"a": 1})
        async for msg in ws:              # iterate frames
            if msg.type == aiohttp.WSMsgType.TEXT:
                print(msg.data)
                break
```

This runs identically under `python hello.py` on your PC and on an ESP32 over WiFi.

## Install

### On a PC (CPython)

No package to install — it's one file. Drop `aiohttp.py` next to your script (or on
`sys.path`) and `import aiohttp`. The repo ships a `pyproject.toml`, so you can also:

```bash
git clone https://github.com/Tangerino/aiohttp.git
cd aiohttp
uv venv && uv pip install -e .          # or: python -m venv .venv && pip install -e .
python test_aiohttp.py                  # runs the suite against real endpoints
```

> Note: `pip install aiohttp` from PyPI gets the **full CPython aiohttp**, a different
> and much larger library. This module is a MicroPython-first reimplementation; use the
> file from this repo.

### On a board (MicroPython)

Copy the single module to the device root and `import aiohttp` finds it:

```bash
mpremote connect /dev/tty.usbserial-1330 cp aiohttp.py :
```

Make sure no package form shadows it — if a previous deploy left `:lib/aiohttp/`, remove
it first (`mpremote ... rm -r :lib/aiohttp`), since a package directory takes precedence
over a same-named `.py` module. (Avoid `mip install aiohttp`: that pulls the upstream
package form, not this single-module copy.)

## Cross-runtime development benefits

The payoff of identical code on both runtimes:

- **Faster iteration.** Develop and debug on CPython — breakpoints, full tracebacks,
  instant restarts — then flash tested code to MicroPython instead of debugging on the
  device.
- **Real network, no hardware.** The PC run hits the same `https://`/`wss://` servers the
  board will, so TLS, redirects, chunked transfer, gzip/deflate and timeouts are all
  exercised before anything touches an ESP32.
- **Consistent behavior.** One API, one set of semantics. The module absorbs the
  per-runtime API differences (gzip via the `deflate` module vs. stdlib `zlib`;
  connection teardown via `reader.aclose()` vs. the writer), so the same call behaves the
  same on both.
- **One codebase across the fleet.** The same client can run on an ESP32 edge device and
  a CPython gateway.

## API reference

### `ClientSession`

```python
ClientSession(base_url="", headers={}, version=HttpVersion11, timeout=None)
```

- `base_url` — prepended to the `url` of every request (e.g. `"https://api.example.com"`).
- `headers` — default headers merged into every request.
- `version` — `HttpVersion11` (default, enables keep-alive) or `HttpVersion10`.
- `timeout` — default per-operation timeout in **seconds** for the whole session
  (`None` = wait forever). Applied to connect, send, **and each read**. Override per call.

Use it as an async context manager so the connection is closed on exit:

```python
async with aiohttp.ClientSession(base_url="https://postman-echo.com", timeout=10) as s:
    async with s.get("/get") as r:
        ...
```

**Request methods** — each returns an async-context-manager yielding a `ClientResponse`:

| Method | |
|--------|--|
| `session.get(url, **kw)` | |
| `session.post(url, **kw)` | |
| `session.put(url, **kw)` | |
| `session.patch(url, **kw)` | |
| `session.delete(url, **kw)` | |
| `session.head(url, **kw)` | |
| `session.options(url, **kw)` | |
| `session.request(method, url, **kw)` | generic |

**Keyword arguments** (`**kw`):

- `params=dict` — appended as a sorted `?k=v&...` query string.
- `json=obj` — serialized to a JSON body with `Content-Type: application/json`.
- `data=str|bytes` — raw body. `bytes` → `application/octet-stream`. (If `data`/`json`
  is given on a `GET`, the method is promoted to `POST`, matching upstream.)
- `headers=dict` — per-request headers, merged over the session defaults.
- `ssl=` — passed through to `asyncio.open_connection` (defaults to `True` for `https`).
- `timeout=` — per-operation timeout in seconds for this call only. Omit to use the
  session default; pass `None` to disable the timeout for this call; pass a number to
  override it.

`session.ws_connect(url, ssl=None)` — open a WebSocket (see below).

Instrumentation: `session.last_reused` is `True` when the last request reused the
keep-alive socket rather than opening a new connection.

### `ClientResponse`

Yielded by `async with session.get(...) as resp`:

- `resp.status` — integer HTTP status.
- `resp.headers` — response headers as a `dict` (case preserved as sent).
- `resp.url` — the final URL (after redirects; includes appended `params`).
- `await resp.read(sz=-1)` — body as `bytes`. Transparently inflates `gzip`/`deflate`
  responses. With keep-alive, `read(-1)` is bounded by `Content-Length` so it can't block.
- `await resp.text(encoding="utf-8")` — body decoded to `str`.
- `await resp.json()` — body parsed from JSON.

Redirects (301–303) with an **absolute** `Location` are followed automatically (up to
two hops). Relative `Location` values are not followed.

### WebSocket — `ClientWebSocketResponse`

Yielded by `async with session.ws_connect(url) as ws`:

- `await ws.send_str(s)` / `await ws.send_bytes(b)` / `await ws.send_json(obj)`
- `await ws.receive_str()` / `await ws.receive_bytes()` / `await ws.receive_json()`
- `async for msg in ws:` — iterate frames; each `msg` has `msg.type` (a `WSMsgType`)
  and `msg.data`.
- `await ws.close()`

`WSMsgType` constants: `TEXT` (1), `BINARY` (2), `ERROR` (258).

### Timeouts

The per-operation timeout is the headline feature for embedded polling: when you poll
HTTP devices on a LAN, some are offline (connect hangs on an unanswered SYN) or wedged
(the response never arrives). The timeout is applied to **every** network wait — connect,
send, and each read — so a single stalled operation aborts the request with
`asyncio.TimeoutError`, and the socket is dropped so a later request can't inherit a
half-spoken connection.

```python
# Default for every request on the session (None = wait forever):
async with aiohttp.ClientSession(timeout=2.0) as s:
    async with s.get("http://192.168.1.50/status") as r:   # offline -> aborts in ~2s
        data = await r.json()

    # Override per request (None disables it for that call only):
    async with s.get("http://192.168.1.50/slow", timeout=0.5) as r:
        ...
```

It is built on `asyncio.wait_for`, which exists on both MicroPython (≥1.13) and CPython,
so the same code times out identically on a board and on a PC.

## Differences from upstream

This module is **derived from the
[`aiohttp`](https://github.com/micropython/micropython-lib/tree/master/python-ecosys/aiohttp)
package in [micropython-lib](https://github.com/micropython/micropython-lib)** by
Carlos Gil. It is a backward-compatible superset — existing call sites keep working —
with these changes:

| Area | Upstream (micropython-lib) | This module |
|------|----------------------------|-------------|
| Packaging | `aiohttp/` package, two modules | single `aiohttp.py` |
| Runtime | MicroPython only | MicroPython **and** CPython (stdlib-only) |
| Timeouts | `# TODO: Implement timeouts` | per-operation timeout (connect/send/read) via `asyncio.wait_for`, session default + per-request override |
| Connection | HTTP/1.0, `Connection: close`, one socket per request | HTTP/1.1 **keep-alive**: one TCP/TLS socket reused per host, with stale-socket detection + one transparent reconnect |
| Body tracking | none | tracks `Content-Length` / bytes read so keep-alive knows when the socket is drained; HEAD and 204/304 pinned to empty body |
| gzip/deflate | MicroPython `deflate` module only (prints a warning if missing) | falls back to stdlib `zlib` on CPython |
| Header parsing | case-**sensitive** match on `Transfer-Encoding:` / `Location:` | case-**insensitive** (handles lowercased headers from go-httpbin / HTTP/2 proxies) |
| Socket teardown | `reader.aclose()` (MicroPython API) | `_aclose()` helper: `reader.aclose()` on MicroPython, writer + `transport.abort()` on CPython |
| Request bytes | bytes-`%` formatting + `writer.awrite()` (MicroPython-only) | build as `str` then `.encode()`, `writer.write()` + `drain()` (works on both) |
| Default version | `HttpVersion10` | `HttpVersion11` |

The HTTP/WebSocket framing, the public `ClientSession` / `ClientResponse` /
`ClientWebSocketResponse` API, and the redirect handling are otherwise unchanged from
upstream.

## Tests

[`test_aiohttp.py`](test_aiohttp.py) is a live functional suite that exercises the client
against real HTTP/WebSocket endpoints. It runs on **both** runtimes — the same file, the
same assertions:

```bash
python test_aiohttp.py                                   # PC (CPython)
mpremote connect PORT mount . run test_aiohttp.py        # board (over WiFi)
```

A remote host that 5xx's or is unreachable after retries, or an endpoint a host does not
provide, is reported as **SKIP** (not FAIL) — those are service issues, not client bugs.
Each check prints a timestamped `PASS`/`FAIL` line and the run ends with a `summary`
counting passed / failed / skipped. On a board each `DONE` line also reports free heap,
so leaks are easy to spot.

| Test | Checks |
|------|--------|
| `get_status` | `GET /get` returns 200 and echoes the URL |
| `get_params` | query params are reflected back |
| `text_json` | `resp.text()` / `resp.json()` readers (Content-Length path) |
| `post_json` | JSON body round-trips |
| `post_data` | raw text body round-trips |
| `post_bytes` | `bytes` body → `application/octet-stream`, round-trips |
| `custom_header` | a custom request header is sent |
| `methods` | GET/PUT/PATCH/DELETE all return 200 |
| `head_options` | HEAD (empty body) and OPTIONS |
| `redirect` | absolute redirect followed (http→https) |
| `compression` | gzip response transparently inflated to valid JSON |
| `deflate` | deflate response inflated |
| `websocket` | text echo (binary/JSON best-effort; text-only servers WARN) |
| `ws_iter` | `async for msg in ws` iteration + `WSMsgType` |
| `speed` | a download streamed in fixed-size chunks + throughput |
| `timeout_connect` | a never-completing connect aborts at the session timeout |
| `timeout_read` | a stalled response aborts at the read timeout |
| `timeout_generous` | a generous timeout does **not** disturb a normal request |
| `timeout_override` | per-request `timeout=` overrides the session default |

### Configuration

All knobs live in the **`CONFIG` block at the top of `test_aiohttp.py`** — edit the
constants directly (no `.env`, no extra files). Set `WIFI_SSID` / `WIFI_PASSWORD` for
on-device runs; on a PC the host network is used and WiFi is skipped. Point `HTTP_BASE` /
`WS_URL` (and `SPEED_URL`, `REDIRECT_URL`, `DEFLATE_URL`) at your own server if you don't
want the defaults.

Two endpoints use a host other than `HTTP_BASE` because postman-echo doesn't provide
them: `REDIRECT_URL` defaults to `http://github.com` (a single http→https redirect
landing on 200), and `DEFLATE_URL` defaults to `https://httpbingo.org/deflate`
(postman-echo's `/deflate` answers 200 *without* actually deflating). Blank either to
skip that test.

The echo-shape assertions are host-agnostic — they accept postman-echo, httpbin, and
go-httpbin response shapes.

### Running on a board

Find your port with `mpremote devs` and pin it via `connect PORT`. Option A mounts this
folder for the run (nothing written to flash, edit-and-rerun instantly):

```bash
mpremote connect /dev/tty.usbserial-1330 mount . run test_aiohttp.py
```

Option B copies to flash (persistent): `cp aiohttp.py : + cp test_aiohttp.py :`, then
`run test_aiohttp.py`.

### Troubleshooting

- **`could not enter raw repl` / port busy** — close any open REPL or serial monitor,
  replug the board, or recheck the `PORT` from `mpremote devs`.
- **`WiFi connection failed`** — wrong `WIFI_SSID`/`WIFI_PASSWORD`, or out of range.
  ESP32 only joins 2.4 GHz networks.
- **`ImportError: no module named 'aiohttp'`** — `aiohttp.py` is not on the device
  `sys.path`. Mount (Option A) or copy it to the board root (Option B).
- **`OSError` / TLS errors on `https`/`wss`** — some boards need more heap or current
  firmware for TLS. Point `HTTP_BASE`/`WS_URL` at plain `http`/`ws` endpoints if needed.
- **Lots of `SKIP` lines** — the configured host is overloaded (5xx) or unreachable; the
  client is fine. Re-run or point the endpoints at a healthier host. (httpbin.org in
  particular is often rate-limited.)

## Credits

Derived from the
[`aiohttp`](https://github.com/micropython/micropython-lib/tree/master/python-ecosys/aiohttp)
package in [micropython-lib](https://github.com/micropython/micropython-lib), originally
written by **Carlos Gil** (MIT), which is itself adapted from
[danni/uwebsockets](https://github.com/danni/uwebsockets) and the WebSocket code in
[miguelgrinberg/microdot](https://github.com/miguelgrinberg/microdot). The single-module
port plus the CPython, timeout, and keep-alive work are by **Carlos Tangerino**. All
original copyright notices are retained in [`aiohttp.py`](aiohttp.py) and
[`LICENSE`](LICENSE). Thanks to the upstream authors.

Developed with AI assistance (Claude Code) — used for the single-module merge, the
CPython/MicroPython compatibility work, the timeout and keep-alive implementation, and
the test suite. API design and the quality bar are owned by the maintainer.

## Related projects

- [aiomqttc](https://github.com/Tangerino/aiomqttc) — async MQTT client that also runs
  unchanged on CPython and MicroPython.
- [mpModbus](https://github.com/Tangerino/mpModbus) — Modbus library (TCP + RTU) with the
  same dual-runtime philosophy and a production-grade data-collection layer.

## License

MIT — see [`LICENSE`](LICENSE). Two copyright holders: Carlos Gil (original upstream) and
Carlos Tangerino (this port). Use it, fork it, ship it; just keep the copyright notice in
derivative source.
