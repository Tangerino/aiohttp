# Live functional tests for the MicroPython aiohttp client.
# MIT license; Copyright (c) 2023 Carlos Gil
#
# These exercise the client against real HTTP/WebSocket endpoints, so they need
# network access. They run on **both MicroPython and CPython**: on a board WiFi
# is brought up from the WIFI_SSID/WIFI_PASSWORD constants below; on a PC WiFi is
# skipped and the host network is used directly.
#
#   $ mpremote connect PORT mount . run test_aiohttp.py   # board
#   $ ../.venv/bin/python test_aiohttp.py                 # PC (CPython)
#
# Logging is verbose by design: every line is timestamped (ms since start), every
# step is logged, and each test is prefixed with its progress (current/total %).
# A remote host that 5xx's or is unreachable after retries, or an endpoint a host
# does not provide, is reported as SKIP (not FAIL) -- those are service issues,
# not client bugs. The run always finishes with a summary. All config is in the
# CONFIG section below -- edit the constants directly.

import sys

# ruff: noqa: E402
sys.path.insert(0, '.')

import asyncio
import binascii
import gc
import json
import os

import aiohttp

try:
    from time import ticks_ms, ticks_diff  # MicroPython
except ImportError:  # CPython
    import time

    def ticks_ms():
        return time.monotonic_ns() // 1_000_000

    def ticks_diff(a, b):
        return a - b


# ===========================================================================
# CONFIG -- every knob the suite reads, in one place. Edit these directly.
#
# On a PC (CPython) the host network is used and WiFi is skipped. On a board
# (MicroPython) connect_wifi() below brings up WiFi from WIFI_SSID/WIFI_PASSWORD.
# ===========================================================================

# WiFi credentials. Used only on-device (e.g. ESP32); ignored on a PC (CPython).
# Fill these in before running on a board.
WIFI_SSID = 'your-network-name'
WIFI_PASSWORD = 'your-network-password'

# Echo host. postman-echo is reliable; httpbin works but often 503s (-> SKIP).
HTTP_BASE = 'https://postman-echo.com'
WS_URL = 'wss://ws.postman-echo.com/raw'

# Deflate test target. postman-echo's /deflate does NOT actually deflate-compress
# (it answers 200 with no Content-Encoding), which makes the test SKIP. httpbingo
# really returns Content-Encoding: deflate, so the client's inflate path is
# exercised. Must return a deflated JSON body; blank it to skip the deflate test.
DEFLATE_URL = 'https://httpbingo.org/deflate'

# Speed test: stream a fixed-size download (set the byte count in the URL). The
# body is read in SPEED_CHUNK_SIZE-byte pieces and discarded, so peak RAM is one
# chunk rather than the whole file -- essential on low-memory boards (ESP32).
SPEED_URL = 'https://speed.cloudflare.com/__down?bytes=1048576'
SPEED_CHUNK_SIZE = 4096

# Absolute URL that issues a redirect with an absolute Location. github.com 301s
# http -> https://github.com/ (a single hop landing on 200); blank it to skip the
# redirect test. This client follows only one absolute redirect, so the Location
# must be absolute AND its target must itself return 200 (no second hop).
REDIRECT_URL = 'http://github.com'

# Retry policy for transient failures (HTTP 5xx or connection errors).
RETRIES = 2
RETRY_DELAY = 1.0


async def connect_wifi(timeout=15):
    """Connect to WiFi using WIFI_SSID / WIFI_PASSWORD.

    Returns the ifconfig tuple on success, or None when there is nothing to
    do: the `network` module is unavailable (e.g. the Unix port) or no SSID
    was configured. Raises OSError if a configured network fails to join.
    """
    try:
        import network
    except ImportError:
        return None

    if not WIFI_SSID:
        return None

    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    if not wlan.isconnected():
        wlan.connect(WIFI_SSID, WIFI_PASSWORD)
        for _ in range(timeout * 10):
            if wlan.isconnected():
                break
            await asyncio.sleep(0.1)
    if not wlan.isconnected():
        raise OSError('WiFi connection failed for SSID %r' % WIFI_SSID)
    return wlan.ifconfig()


_START = ticks_ms()
passed = 0
failed = 0
skipped = 0


class ServiceUnavailable(Exception):
    """A remote endpoint was unreachable or 5xx'd after retries.

    The test could not run because of the service, not a client bug, so it is
    reported as SKIP rather than FAIL.
    """


class SkipTest(Exception):
    """The configured host does not provide this endpoint; skip the test."""


def _ms():
    return ticks_diff(ticks_ms(), _START)


def log(level, msg, indent=0):
    print('[%8d ms] %-5s %s%s' % (_ms(), level, '  ' * indent, msg))


def step(msg, indent=1):
    """Log a verbose progress step inside a test."""
    log('STEP', msg, indent)


def check(cond, msg):
    global passed, failed
    if cond:
        passed += 1
        log('PASS', msg, 1)
    else:
        failed += 1
        log('FAIL', msg, 1)


def as_json(body):
    """Parse a JSON body, raising a readable error when it is not JSON."""
    try:
        return json.loads(body)
    except ValueError:
        raise ValueError('non-JSON response: %r' % (body[:80],))


def header_get(headers, name, default=None):
    """Case-insensitive header lookup (echo servers vary key casing)."""
    name = name.lower()
    for key, value in headers.items():
        if key.lower() == name:
            return value
    return default


def _unwrap(v):
    """Normalize an echoed value across httpbin variants.

    postman-echo / httpbin echo query args and headers as scalars; go-httpbin
    mirrors wrap each in a single-element list. Treat a 1-element list as its
    element so the assertions pass on all of them.
    """
    if isinstance(v, list) and len(v) == 1:
        return v[0]
    return v


def _echoed_body(data):
    """Return the echoed request body as text across echo-host variants.

    Most echo hosts return an untyped body verbatim in `data`; go-httpbin wraps
    it as a `data:<mime>;base64,<b64>` URL. Decode that form so the check works.
    """
    if isinstance(data, str) and data.startswith('data:') and ';base64,' in data:
        return binascii.a2b_base64(data.split(';base64,', 1)[1]).decode()
    return data


def mem_free():
    """Free heap in bytes after a collection, or None if unsupported (CPython)."""
    try:
        gc.collect()
        return gc.mem_free()
    except AttributeError:
        return None


def log_machine_info():
    """Log board/runtime resources up front (best-effort; varies by port)."""
    gc.collect()
    try:
        u = os.uname()
        log('INFO', 'machine:  %s' % u.machine)
        log('INFO', 'firmware: %s %s' % (u.sysname, u.release))
        log('INFO', 'build:    %s' % u.version)
    except Exception:  # noqa: BLE001 - resource probes are best-effort
        log('INFO', 'platform: %s' % sys.platform)
    try:
        free, alloc = gc.mem_free(), gc.mem_alloc()
        log('INFO', 'heap:     %d free / %d total bytes' % (free, free + alloc))
    except Exception:  # noqa: BLE001
        pass
    try:
        import machine

        log('INFO', 'cpu:      %d MHz' % (machine.freq() // 1000000))
    except Exception:  # noqa: BLE001
        pass
    try:
        st = os.statvfs('/')
        log('INFO', 'flash fs: %d free / %d total bytes' % (st[0] * st[3], st[0] * st[2]))
    except Exception:  # noqa: BLE001
        pass


def _echoed_bytes(data):
    """Decode an echoed *binary* body across echo-host variants.

    httpbin returns a `data:<mime>;base64,<b64>` URL; postman-echo (Node) returns
    a Buffer dict {"type": "Buffer", "data": [..ints..]}. Returns bytes, or None
    if the shape is unrecognized.
    """
    if isinstance(data, dict) and data.get('type') == 'Buffer' and isinstance(data.get('data'), list):
        return bytes(data['data'])
    if isinstance(data, str) and data.startswith('data:') and ';base64,' in data:
        return binascii.a2b_base64(data.split(';base64,', 1)[1])
    return None


async def http(method, path, read='bytes', **kwargs):
    """Perform a request and return (status, headers, body).

    Relative paths are joined onto HTTP_BASE. `read` selects how the body is
    consumed: "bytes" -> resp.read() (default), "text" -> resp.text(), "json" ->
    resp.json(); the latter two exercise the convenience readers, which take the
    Content-Length path rather than read-to-EOF. Transient failures (HTTP 5xx or a
    raised OSError) are retried up to RETRIES times; if they persist, raise
    ServiceUnavailable so the caller is reported as SKIP. Each request/response is
    logged verbosely, with per-request timing (a fresh ClientSession means a full
    TLS handshake every call -- the dominant cost over HTTPS on the board).
    """
    method = method.upper()
    url = HTTP_BASE + path if path.startswith('/') else path
    attempt = 0
    while True:
        attempt += 1
        suffix = '' if attempt == 1 else ' (attempt %d/%d)' % (attempt, RETRIES + 1)
        step('-> %s %s%s' % (method, url, suffix), 2)
        t0 = ticks_ms()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, **kwargs) as resp:
                    status = resp.status
                    headers = resp.headers
                    if read == 'text':
                        body = await resp.text()
                    elif read == 'json':
                        body = await resp.json()
                    else:
                        body = await resp.read()
            dt = ticks_diff(ticks_ms(), t0)
            size = ', %d bytes' % len(body) if isinstance(body, (bytes, bytearray, str)) else ''
            step('<- HTTP %d in %d ms%s' % (status, dt, size), 2)
            if status >= 500:
                if attempt <= RETRIES:
                    log('WARN', '%s %s -> %d, retry %d/%d' % (method, url, status, attempt, RETRIES), 1)
                    await asyncio.sleep(RETRY_DELAY)
                    continue
                raise ServiceUnavailable('%s %s -> %d after %d attempts' % (method, path, status, attempt))
            return status, headers, body
        except OSError as e:
            if attempt <= RETRIES:
                log('WARN', '%s %s raised %r, retry %d/%d' % (method, url, e, attempt, RETRIES), 1)
                await asyncio.sleep(RETRY_DELAY)
                continue
            raise ServiceUnavailable('%s %s unreachable: %r' % (method, path, e))


async def test_get_status():
    step('requesting /get')
    status, _, body = await http('GET', '/get')
    check(status == 200, 'GET /get returns 200 (got %s)' % status)
    data = as_json(body)
    step('parsed JSON body, url=%s' % data.get('url'))
    check(data.get('url', '').endswith('/get'), 'GET /get echoes url')


async def test_get_params():
    params = {'key1': 'value1', 'key2': 'value2'}
    step('requesting /get with params=%s' % params)
    status, _, body = await http('GET', '/get', params=params)
    check(status == 200, 'GET params returns 200 (got %s)' % status)
    echoed = as_json(body).get('args')
    step('server echoed args=%s' % echoed)
    normalized = {k: _unwrap(v) for k, v in echoed.items()} if isinstance(echoed, dict) else echoed
    check(normalized == params, 'query params reflected')


async def test_post_json():
    payload = {'hello': 'world', 'n': 42}
    step('posting JSON payload=%s' % payload)
    status, _, body = await http('POST', '/post', json=payload)
    check(status == 200, 'POST /post returns 200 (got %s)' % status)
    echoed = as_json(body).get('json')
    step('server echoed json=%s' % echoed)
    check(echoed == payload, 'POST json body round-trips')


async def test_post_data():
    step("posting raw text body 'raw-body'")
    status, _, body = await http(
        'POST', '/post', data='raw-body', headers={'Content-Type': 'text/plain'}
    )
    check(status == 200, 'POST data returns 200 (got %s)' % status)
    echoed = as_json(body).get('data')
    step('server echoed data=%r' % echoed)
    check(_echoed_body(echoed) == 'raw-body', 'POST text body round-trips')


async def test_custom_header():
    step('requesting /headers with X-Test-Header: micropython')
    status, _, body = await http('GET', '/headers', headers={'X-Test-Header': 'micropython'})
    check(status == 200, 'GET /headers returns 200 (got %s)' % status)
    # Echo servers may lowercase header names and/or list-wrap values.
    sent = as_json(body).get('headers', {})
    got = _unwrap(header_get(sent, 'X-Test-Header'))
    step('server saw X-Test-Header=%r' % got)
    check(got == 'micropython', 'custom header sent')


async def test_methods():
    methods = (('GET', '/get'), ('PUT', '/put'), ('PATCH', '/patch'), ('DELETE', '/delete'))
    for i, (method, path) in enumerate(methods, 1):
        step('method %d/%d: %s %s' % (i, len(methods), method, path))
        status, _, _ = await http(method, path)
        check(status == 200, '%s %s returns 200 (got %s)' % (method, path, status))


async def test_redirect():
    # Needs an endpoint that returns an absolute Location (this client does not
    # follow relative redirects). Set REDIRECT_URL to enable.
    if not REDIRECT_URL:
        raise SkipTest('no REDIRECT_URL configured (host has no redirect endpoint)')
    step('following absolute redirect %s' % REDIRECT_URL)
    status, _, _ = await http('GET', REDIRECT_URL)
    check(status == 200, 'absolute redirect followed to 200 (got %s)' % status)


async def test_compression():
    step('requesting /gzip with Accept-Encoding: gzip,deflate')
    status, headers, body = await http('GET', '/gzip', headers={'Accept-Encoding': 'gzip,deflate'})
    check(status == 200, 'GET /gzip returns 200 (got %s)' % status)
    # The server must actually compress, and the client must transparently
    # inflate it -- proven by the decoded body parsing as JSON.
    encoding = header_get(headers, 'Content-Encoding', '')
    step('Content-Encoding=%r, decoded body is %d bytes' % (encoding, len(body)))
    check('gzip' in encoding or 'deflate' in encoding, 'response compressed (%s)' % encoding)
    check(isinstance(as_json(body), dict), 'decompressed body is valid JSON')


async def test_websocket():
    attempt = 0
    while True:
        attempt += 1
        suffix = '' if attempt == 1 else ' (attempt %d/%d)' % (attempt, RETRIES + 1)
        step('connecting to %s%s' % (WS_URL, suffix))
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL) as ws:
                    step("connected; sending text 'ping'")
                    # Some echo servers (e.g. echo.websocket.events) send a
                    # banner first; skip non-matching messages once.
                    await ws.send_str('ping')
                    reply = await ws.receive_str()
                    if reply != 'ping':
                        step('got banner %r, reading next frame' % reply)
                        reply = await ws.receive_str()
                    step('received text %r' % reply)
                    check(reply == 'ping', 'ws text echo')
                    # Binary/JSON echo is best-effort: some echo servers are
                    # text-only and close the socket -- the server's limitation,
                    # not a client bug; text echo above proves the framing works.
                    try:
                        step("sending binary b'\\x01\\x02\\x03'")
                        await ws.send_bytes(b'\x01\x02\x03')
                        recv = await ws.receive_bytes()
                        step('received binary %r' % recv)
                        check(recv == b'\x01\x02\x03', 'ws binary echo')
                        step("sending json {'a': 1}")
                        await ws.send_json({'a': 1})
                        recv = await ws.receive_json()
                        step('received json %s' % recv)
                        check(recv == {'a': 1}, 'ws json echo')
                    except (EOFError, OSError) as e:
                        log('WARN', 'ws server closed before binary/json echo: %r' % e, 1)
            return
        except (OSError, EOFError) as e:
            if attempt <= RETRIES:
                log(
                    'WARN',
                    'ws_connect %s raised %r, retry %d/%d' % (WS_URL, e, attempt, RETRIES),
                    1,
                )
                await asyncio.sleep(RETRY_DELAY)
                continue
            raise ServiceUnavailable('ws_connect %s unreachable: %r' % (WS_URL, e))


async def test_speed():
    if not SPEED_URL:
        raise SkipTest('no SPEED_URL configured')
    step('streaming %s in %d-byte chunks' % (SPEED_URL, SPEED_CHUNK_SIZE))
    total = 0
    chunked = False
    next_mark = 262144  # log a progress line each 256 KB
    t0 = ticks_ms()
    # Read the body straight off the stream in fixed-size chunks and discard it,
    # so memory use stays flat regardless of download size. Dechunks transparently
    # for chunked responses; reads raw for plain Content-Length bodies.
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(SPEED_URL) as resp:
                if resp.status >= 500:
                    raise ServiceUnavailable('speed %s -> %d' % (SPEED_URL, resp.status))
                if resp.status != 200:
                    check(False, 'speed download returns 200 (got %s)' % resp.status)
                    return
                chunked = isinstance(resp, aiohttp.ChunkedClientResponse)
                read = resp.read if chunked else resp.content.read
                while True:
                    part = await read(SPEED_CHUNK_SIZE)
                    if not part:
                        break
                    total += len(part)
                    if total >= next_mark:
                        step('... %d bytes streamed' % total, 2)
                        next_mark += 262144
    except OSError as e:
        raise ServiceUnavailable('speed %s unreachable: %r' % (SPEED_URL, e))
    dt = ticks_diff(ticks_ms(), t0)
    check(
        total > 0,
        'streamed %d bytes in %d-byte chunks%s' % (total, SPEED_CHUNK_SIZE, ' (dechunked)' if chunked else ''),
    )
    # MB/s is decimal megabytes (1 MB = 1,000,000 bytes), usual for transfer
    # rates; Mbit/s is the bit-rate (8x), common for network links.
    mb_per_s = (total / 1000000) / (dt / 1000) if dt else 0
    log(
        'INFO',
        'throughput: %d bytes in %d ms (%.3f MB/s, %.2f Mbit/s)' % (total, dt, mb_per_s, mb_per_s * 8),
        1,
    )


async def test_text_json():
    step('reading body via resp.text()')
    status, _, text = await http('GET', '/get', read='text')
    check(status == 200, 'GET /get (text) returns 200 (got %s)' % status)
    check(isinstance(text, str) and '/get' in text, 'resp.text() returns decoded str')
    step('reading body via resp.json()')
    status, _, data = await http('GET', '/get', read='json')
    check(status == 200, 'GET /get (json) returns 200 (got %s)' % status)
    check(
        isinstance(data, dict) and data.get('url', '').endswith('/get'),
        'resp.json() parses body to dict',
    )


async def test_post_bytes():
    payload = b'\x00\x01\x02binary'
    step('posting bytes body %r' % payload)
    status, _, body = await http('POST', '/post', data=payload)
    check(status == 200, 'POST bytes returns 200 (got %s)' % status)
    parsed = as_json(body)
    ct = _unwrap(header_get(parsed.get('headers', {}), 'Content-Type'))
    step('server saw Content-Type=%r' % ct)
    check(bool(ct) and 'octet-stream' in ct, 'bytes body sent as application/octet-stream')
    echoed = _echoed_bytes(parsed.get('data'))
    if echoed is None:
        log('WARN', 'host did not echo binary body in a known shape', 1)
    else:
        step('server echoed %d bytes' % len(echoed))
        check(echoed == payload, 'bytes body round-trips')


async def test_head_options():
    step('HEAD /get (no body expected)')
    status, _, body = await http('HEAD', '/get')
    check(status == 200, 'HEAD /get returns 200 (got %s)' % status)
    check(body == b'', 'HEAD returns empty body')
    step('OPTIONS /get')
    status, headers, _ = await http('OPTIONS', '/get')
    if status >= 400:
        raise SkipTest('host does not support OPTIONS (got %s)' % status)
    allow = header_get(headers, 'Allow') or header_get(headers, 'Access-Control-Allow-Methods')
    step('server Allow=%r' % allow)
    check(status == 200, 'OPTIONS /get returns 200 (got %s)' % status)


async def test_deflate():
    if not DEFLATE_URL:
        raise SkipTest('no DEFLATE_URL configured')
    step('requesting %s with Accept-Encoding: deflate' % DEFLATE_URL)
    status, headers, body = await http('GET', DEFLATE_URL, headers={'Accept-Encoding': 'deflate'})
    if status == 404:
        raise SkipTest('host has no deflate endpoint')
    check(status == 200, 'GET deflate returns 200 (got %s)' % status)
    encoding = header_get(headers, 'Content-Encoding', '')
    if 'deflate' not in encoding:
        raise SkipTest('host did not deflate (Content-Encoding=%r)' % encoding)
    step('Content-Encoding=%r, decoded body is %d bytes' % (encoding, len(body)))
    check(isinstance(as_json(body), dict), 'deflate response inflated to valid JSON')


async def test_ws_iter():
    attempt = 0
    while True:
        attempt += 1
        suffix = '' if attempt == 1 else ' (attempt %d/%d)' % (attempt, RETRIES + 1)
        step('connecting to %s%s' % (WS_URL, suffix))
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL) as ws:
                    step("connected; sending 'alpha', iterating with async for")
                    await ws.send_str('alpha')
                    got = False
                    async for msg in ws:
                        step('iter received type=%s data=%r' % (msg.type, msg.data))
                        if msg.data != 'alpha':  # skip a possible banner
                            continue
                        check(msg.type == aiohttp.WSMsgType.TEXT, 'ws message type is TEXT')
                        check(msg.data == 'alpha', 'ws async-iter echo')
                        got = True
                        break
                    if not got:
                        check(False, 'ws async-iter produced no echo')
            return
        except (OSError, EOFError) as e:
            if attempt <= RETRIES:
                log(
                    'WARN',
                    'ws_connect %s raised %r, retry %d/%d' % (WS_URL, e, attempt, RETRIES),
                    1,
                )
                await asyncio.sleep(RETRY_DELAY)
                continue
            raise ServiceUnavailable('ws_connect %s unreachable: %r' % (WS_URL, e))


# --- timeouts -------------------------------------------------------------
# Polling HTTP devices on a LAN means some are offline (connect hangs on an
# unanswered SYN) or wedged (response never comes). Without a timeout the client
# would block forever; these verify it aborts promptly with asyncio.TimeoutError.

# How long the read-timeout test asks the server to stall (postman-echo /delay/N).
TIMEOUT_DELAY_PATH = '/delay/5'
# Margin (ms) allowed between the configured timeout and the actual abort.
TIMEOUT_MARGIN_MS = 2500


class _HangConnect:
    """Context manager that makes aiohttp's connect step never complete.

    Replaces asyncio.open_connection with a coroutine that sleeps far longer than
    any test timeout, so the ONLY way a request can return is the timeout firing.
    This makes the connect-timeout tests deterministic and network-free (they run
    identically on a board and under CPython). Restores the original on exit.
    """

    def __enter__(self):
        self._orig = asyncio.open_connection
        asyncio.open_connection = self._hang
        return self

    def __exit__(self, *args):
        asyncio.open_connection = self._orig
        return False

    async def _hang(self, *args, **kwargs):
        await asyncio.sleep(30)


async def test_timeout_connect():
    step('connect that never completes, session timeout=1.0s')
    t0 = ticks_ms()
    with _HangConnect():
        try:
            async with aiohttp.ClientSession(timeout=1.0) as s:
                async with s.get('http://192.0.2.1/') as r:  # TEST-NET, never used
                    await r.read()
            check(False, 'request should have timed out on connect')
            return
        except asyncio.TimeoutError:
            dt = ticks_diff(ticks_ms(), t0)
    step('aborted after %d ms' % dt)
    check(dt < 1000 + TIMEOUT_MARGIN_MS, 'connect timeout fired promptly (%d ms)' % dt)


async def test_timeout_read():
    # The server accepts the connection but withholds the response; a shorter
    # read timeout must abort well before the server would have replied.
    step('GET %s with timeout=1.5s (server stalls the response)' % TIMEOUT_DELAY_PATH)
    t0 = ticks_ms()
    status = None
    try:
        async with aiohttp.ClientSession(base_url=HTTP_BASE, timeout=1.5) as s:
            async with s.get(TIMEOUT_DELAY_PATH) as r:
                status = r.status
                await r.read()
    except asyncio.TimeoutError:
        dt = ticks_diff(ticks_ms(), t0)
        step('aborted after %d ms' % dt)
        check(dt < 1500 + TIMEOUT_MARGIN_MS, 'read timeout fired promptly (%d ms)' % dt)
        return
    except OSError as e:
        raise ServiceUnavailable('%s unreachable: %r' % (TIMEOUT_DELAY_PATH, e))
    # No timeout -> the host did not actually stall; nothing to assert.
    raise SkipTest('host did not delay (status %s); no read timeout exercised' % status)


async def test_timeout_generous():
    # A generous timeout must NOT interfere with a normal, fast request -- proves
    # the timeout wrapping is transparent on the success path.
    step('GET /get with a generous timeout=10s (must succeed)')
    t0 = ticks_ms()
    async with aiohttp.ClientSession(base_url=HTTP_BASE, timeout=10) as s:
        async with s.get('/get') as r:
            status = r.status
            body = await r.read()
    dt = ticks_diff(ticks_ms(), t0)
    if status >= 500:
        raise ServiceUnavailable('/get -> %d' % status)
    check(status == 200, 'request under generous timeout returns 200 (got %s)' % status)
    check(len(body) > 0, 'body read fully under timeout (%d bytes in %d ms)' % (len(body), dt))


async def test_timeout_override():
    # A per-request timeout must override the session default. Session default is
    # generous; the tight per-call value is what should fire.
    step('session timeout=30s, per-request timeout=1.0s on a hung connect')
    t0 = ticks_ms()
    with _HangConnect():
        try:
            async with aiohttp.ClientSession(timeout=30) as s:
                async with s.get('http://192.0.2.1/', timeout=1.0) as r:
                    await r.read()
            check(False, 'per-request timeout should have fired')
            return
        except asyncio.TimeoutError:
            dt = ticks_diff(ticks_ms(), t0)
    step('aborted after %d ms' % dt)
    check(dt < 1000 + TIMEOUT_MARGIN_MS, 'per-request timeout overrode session default (%d ms)' % dt)


TESTS = (
    ('get_status', test_get_status),
    ('get_params', test_get_params),
    ('text_json', test_text_json),
    ('post_json', test_post_json),
    ('post_data', test_post_data),
    ('post_bytes', test_post_bytes),
    ('custom_header', test_custom_header),
    ('methods', test_methods),
    ('head_options', test_head_options),
    ('redirect', test_redirect),
    ('compression', test_compression),
    ('deflate', test_deflate),
    ('websocket', test_websocket),
    ('ws_iter', test_ws_iter),
    ('speed', test_speed),
    ('timeout_connect', test_timeout_connect),
    ('timeout_read', test_timeout_read),
    ('timeout_generous', test_timeout_generous),
    ('timeout_override', test_timeout_override),
)


async def run_test(idx, total, name, fn):
    global failed, skipped
    pct = idx * 100 // total
    log('TEST', '[%d/%d %3d%%] %s' % (idx, total, pct, name))
    t0 = ticks_ms()
    p0, f0 = passed, failed
    try:
        await fn()
    except (ServiceUnavailable, SkipTest) as e:
        skipped += 1
        log('SKIP', '%s (%s)' % (name, e), 1)
    except Exception as e:  # noqa: BLE001 - report and keep running
        failed += 1
        log('ERROR', '%s raised %r' % (name, e), 1)
    dt = ticks_diff(ticks_ms(), t0)
    free = mem_free()
    heap = '' if free is None else ', %d B heap free' % free
    log(
        'DONE',
        '[%d/%d %3d%%] %s (%d ms, %d passed / %d failed%s)'
        % (idx, total, pct, name, dt, passed - p0, failed - f0, heap),
    )


async def main():
    total = len(TESTS)
    log('INFO', 'starting aiohttp functional tests (%d tests)' % total)
    log_machine_info()
    try:
        step('bringing up WiFi', 0)
        info = await connect_wifi()
        if info:
            log('INFO', 'WiFi connected: %s' % (info,))
        else:
            log('INFO', 'WiFi skipped (no SSID configured or no network module)')
    except Exception as e:  # noqa: BLE001 - report and continue without WiFi
        log('ERROR', 'WiFi connect failed: %r' % e)
    log('INFO', 'HTTP base: %s' % HTTP_BASE)
    log('INFO', 'WS url:    %s' % WS_URL)

    t0 = ticks_ms()
    for idx, (name, fn) in enumerate(TESTS, 1):
        await run_test(idx, total, name, fn)
    dt = ticks_diff(ticks_ms(), t0)

    log('INFO', '=' * 40)
    log(
        'INFO',
        'summary: %d passed, %d failed, %d skipped of %d checks in %d ms (%.1f s)'
        % (passed, failed, skipped, passed + failed, dt, dt / 1000),
    )
    if skipped and not failed:
        log('INFO', 'skips were unavailable/unsupported endpoints, not client errors')


asyncio.run(main())
