# MicroPython aiohttp library -- single-module HTTP + WebSocket client
#
# SPDX-License-Identifier: MIT
# Copyright (c) 2023 Carlos Gil  (original upstream author)
# Copyright (c) 2026 Carlos Tangerino  (single-module port, CPython support,
#                                       timeouts, HTTP/1.1 keep-alive)
#
# Derived from the `aiohttp` package in micropython-lib:
#   https://github.com/micropython/micropython-lib/tree/master/python-ecosys/aiohttp
# which is itself adapted from:
#   https://github.com/danni/uwebsockets
#   https://github.com/miguelgrinberg/microdot/blob/main/src/microdot_asyncio_websocket.py
#
# This file unifies the upstream package's two modules (__init__.py +
# aiohttp_ws.py) into ONE module, so there is no duplicate copy to keep in sync
# and it deploys as a single `aiohttp.py` (no subpackage folder -- handy under
# OTA constraints). Beyond the merge it diverges substantially from upstream:
#   * runs on BOTH MicroPython and CPython using only each runtime's stdlib;
#   * per-operation timeouts (connect/send/read) via asyncio.wait_for;
#   * HTTP/1.1 keep-alive -- one TCP/TLS socket reused across requests to the
#     same host, with stale-socket detection and one transparent reconnect;
#   * gzip/deflate inflate falls back to stdlib `zlib` when the MicroPython
#     `deflate` module is absent;
#   * case-insensitive response header parsing; Content-Length body tracking.
# See README.md ("Differences from upstream") for the full list.

import asyncio
import binascii
import json as _json
import random
import re
import struct
import sys
from collections import namedtuple

try:
    import socket
except ImportError:
    import usocket as socket

# --- Self-contained portability shims (stdlib only; MicroPython + CPython) -------------
# This module has no third-party dependencies. Everything below is derived from each
# runtime's own stdlib so the single file stays drop-in.

try:
    # MicroPython: monotonic millisecond ticks with wrap-safe diff.
    from time import ticks_diff, ticks_ms
except ImportError:
    # CPython fallback.
    from time import monotonic as _monotonic

    def ticks_ms():
        return int(_monotonic() * 1000)

    def ticks_diff(a, b):
        return a - b

try:
    import errno as _errno
except ImportError:
    import uerrno as _errno

# Socket "would block / retry, not a real error" codes. EAGAIN/EINPROGRESS/ETIMEDOUT are
# standard; 118/119 are ESP32 lwip quirks; 10035 is Windows WSAEWOULDBLOCK.
_BUSY_ERRORS = (
    getattr(_errno, 'EAGAIN', 11),
    getattr(_errno, 'EINPROGRESS', 115),
    getattr(_errno, 'ETIMEDOUT', 110),
    118,
    119,
    10035,
)

# True on any MicroPython port. The greedy-drain read fast path (see ClientResponse) only
# applies here -- CPython's asyncio StreamReader has no equivalent stall, so it keeps the
# builtin readexactly() path.
_IS_MICROPYTHON = sys.implementation.name == 'micropython'


async def _sleep_ms(ms):
    # asyncio.sleep accepts a float on both runtimes; avoids depending on MicroPython's sleep_ms.
    await asyncio.sleep(ms / 1000)


# Optional logging hook for connection diagnostics (DNS / connect / TLS / keep-alive).
# Defaults to a no-op so the library is silent unless a host app opts in via set_logger().
def _noop_log(msg):
    pass


_log = _noop_log


def set_logger(fn):
    """Install a logging callback ``fn(msg: str)`` for connection diagnostics, or None to disable.

    The callback receives one-line, human-readable strings such as
    ``[HTTP] connecting host:443 ssl=True dns=5ms -> 1.2.3.4``. Useful for diagnosing slow
    DNS, TLS handshakes or keep-alive churn. No-op by default.
    """
    global _log
    _log = fn if fn is not None else _noop_log


# ===========================================================================
# websocket support (was aiohttp/aiohttp_ws.py)
# ===========================================================================

URL_RE = re.compile(r'(wss|ws)://([A-Za-z0-9-\.]+)(?:\:([0-9]+))?(/.+)?')
URI = namedtuple('URI', ('protocol', 'hostname', 'port', 'path'))  # noqa: PYI024


async def _aclose(reader, writer):
    """Close a connection opened with asyncio.open_connection on either runtime.

    MicroPython's StreamReader exposes aclose() (closing the shared underlying
    stream); CPython has no reader.aclose(), so we close via the writer. Duck
    typing on `aclose` is the runtime switch -- no version check needed.
    """
    aclose = getattr(reader, 'aclose', None)
    if aclose is not None:
        await aclose()
    elif writer is not None:
        writer.close()
        # _aclose is only ever called on a connection we are discarding, so a
        # graceful TLS close_notify handshake buys nothing -- and on CPython it can
        # block in wait_closed() waiting for an unresponsive peer (e.g. when tearing
        # down mid-response after a timeout), which would swallow the timeout's
        # speed. Force the transport shut so cleanup is immediate.
        transport = getattr(writer, 'transport', None)
        if transport is not None:
            abort = getattr(transport, 'abort', None)
            if abort is not None:
                abort()
        try:
            await writer.wait_closed()
        except (OSError, ConnectionError):
            pass


def urlparse(uri):
    """Parse ws:// URLs"""
    match = URL_RE.match(uri)
    if match:
        protocol = match.group(1)
        host = match.group(2)
        port = match.group(3)
        path = match.group(4)

        if protocol == 'wss':
            if port is None:
                port = 443
        elif protocol == 'ws':
            if port is None:
                port = 80
        else:
            raise ValueError('Scheme {} is invalid'.format(protocol))

        return URI(protocol, host, int(port), path)


class WebSocketMessage:
    def __init__(self, opcode, data):
        self.type = opcode
        self.data = data


class WSMsgType:
    TEXT = 1
    BINARY = 2
    ERROR = 258


class WebSocketClient:
    CONT = 0
    TEXT = 1
    BINARY = 2
    CLOSE = 8
    PING = 9
    PONG = 10

    def __init__(self, params):
        self.params = params
        self.closed = False
        self.reader = None
        self.writer = None

    async def connect(self, uri, ssl=None, handshake_request=None):
        uri = urlparse(uri)
        assert uri
        if uri.protocol == 'wss':
            if not ssl:
                ssl = True
        await self.handshake(uri, ssl, handshake_request)

    @classmethod
    def _parse_frame_header(cls, header):
        byte1, byte2 = struct.unpack('!BB', header)

        # Byte 1: FIN(1) _(1) _(1) _(1) OPCODE(4)
        fin = bool(byte1 & 0x80)
        opcode = byte1 & 0x0F

        # Byte 2: MASK(1) LENGTH(7)
        mask = bool(byte2 & (1 << 7))
        length = byte2 & 0x7F

        return fin, opcode, mask, length

    def _process_websocket_frame(self, opcode, payload):
        if opcode == self.TEXT:
            payload = str(payload, 'utf-8')
        elif opcode == self.BINARY:
            pass
        elif opcode == self.CLOSE:
            # raise OSError(32, "Websocket connection closed")
            return opcode, payload
        elif opcode == self.PING:
            return self.PONG, payload
        elif opcode == self.PONG:  # pragma: no branch
            return None, None
        return None, payload

    @classmethod
    def _encode_websocket_frame(cls, opcode, payload):
        if opcode == cls.TEXT:
            payload = payload.encode()

        length = len(payload)
        fin = mask = True

        # Frame header
        # Byte 1: FIN(1) _(1) _(1) _(1) OPCODE(4)
        byte1 = 0x80 if fin else 0
        byte1 |= opcode

        # Byte 2: MASK(1) LENGTH(7)
        byte2 = 0x80 if mask else 0

        if length < 126:  # 126 is magic value to use 2-byte length header
            byte2 |= length
            frame = struct.pack('!BB', byte1, byte2)

        elif length < (1 << 16):  # Length fits in 2-bytes
            byte2 |= 126  # Magic code
            frame = struct.pack('!BBH', byte1, byte2, length)

        elif length < (1 << 64):
            byte2 |= 127  # Magic code
            frame = struct.pack('!BBQ', byte1, byte2, length)

        else:
            raise ValueError

        # Mask is 4 bytes
        mask_bits = struct.pack('!I', random.getrandbits(32))
        frame += mask_bits
        payload = bytes(b ^ mask_bits[i % 4] for i, b in enumerate(payload))
        return frame + payload

    async def handshake(self, uri, ssl, req):
        headers = self.params
        _http_proto = 'http' if uri.protocol != 'wss' else 'https'
        url = f"{_http_proto}://{uri.hostname}:{uri.port}{uri.path or '/'}"
        key = binascii.b2a_base64(bytes(random.getrandbits(8) for _ in range(16)))[:-1]
        headers['Host'] = f'{uri.hostname}:{uri.port}'
        headers['Connection'] = 'Upgrade'
        headers['Upgrade'] = 'websocket'
        headers['Sec-WebSocket-Key'] = str(key, 'utf-8')
        headers['Sec-WebSocket-Version'] = '13'
        headers['Origin'] = f'{_http_proto}://{uri.hostname}:{uri.port}'

        self.reader, self.writer = await req(
            'GET',
            url,
            ssl=ssl,
            headers=headers,
            is_handshake=True,
            version='HTTP/1.1',
        )

        header = await self.reader.readline()
        header = header[:-2]
        assert header.startswith(b'HTTP/1.1 101 '), header

        while header:
            header = await self.reader.readline()
            header = header[:-2]

    async def receive(self):
        while True:
            opcode, payload, final = await self._read_frame()
            while not final:
                # original opcode must be preserved
                _, morepayload, final = await self._read_frame()
                payload += morepayload
            send_opcode, data = self._process_websocket_frame(opcode, payload)
            if send_opcode:  # pragma: no cover
                await self.send(data, send_opcode)
            if opcode == self.CLOSE:
                self.closed = True
                return opcode, data
            elif data:  # pragma: no branch
                return opcode, data

    async def send(self, data, opcode=None):
        frame = self._encode_websocket_frame(
            opcode or (self.TEXT if isinstance(data, str) else self.BINARY), data
        )
        self.writer.write(frame)
        await self.writer.drain()

    async def close(self):
        if not self.closed:  # pragma: no cover
            self.closed = True
            await self.send(b'', self.CLOSE)

    async def _read_frame(self):
        header = await self.reader.readexactly(2)
        if len(header) != 2:  # pragma: no cover
            # raise OSError(32, "Websocket connection closed")
            opcode = self.CLOSE
            payload = b''
            return opcode, payload
        fin, opcode, has_mask, length = self._parse_frame_header(header)
        if length == 126:  # Magic number, length header is 2 bytes
            (length,) = struct.unpack('!H', await self.reader.readexactly(2))
        elif length == 127:  # Magic number, length header is 8 bytes
            (length,) = struct.unpack('!Q', await self.reader.readexactly(8))

        if has_mask:  # pragma: no cover
            mask = await self.reader.readexactly(4)
        payload = await self.reader.readexactly(length)
        if has_mask:  # pragma: no cover
            payload = bytes(x ^ mask[i % 4] for i, x in enumerate(payload))
        return opcode, payload, fin


class ClientWebSocketResponse:
    def __init__(self, wsclient):
        self.ws = wsclient

    def __aiter__(self):
        return self

    async def __anext__(self):
        msg = WebSocketMessage(*await self.ws.receive())
        # print(msg.data, msg.type) # DEBUG
        if (not msg.data and msg.type == self.ws.CLOSE) or self.ws.closed:
            raise StopAsyncIteration
        return msg

    async def close(self):
        await self.ws.close()

    async def send_str(self, data):
        if not isinstance(data, str):
            raise TypeError('data argument must be str (%r)' % type(data))
        await self.ws.send(data)

    async def send_bytes(self, data):
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError('data argument must be byte-ish (%r)' % type(data))
        await self.ws.send(data)

    async def send_json(self, data):
        await self.send_str(_json.dumps(data))

    async def receive_str(self):
        msg = WebSocketMessage(*await self.ws.receive())
        if msg.type != self.ws.TEXT:
            raise TypeError('Received message %s:%r is not str' % (msg.type, msg.data))
        return msg.data

    async def receive_bytes(self):
        msg = WebSocketMessage(*await self.ws.receive())
        if msg.type != self.ws.BINARY:
            raise TypeError('Received message %s:%r is not bytes' % (msg.type, msg.data))
        return msg.data

    async def receive_json(self):
        data = await self.receive_str()
        return _json.loads(data)


class _WSRequestContextManager:
    def __init__(self, client, request_co):
        self.reqco = request_co
        self.client = client

    async def __aenter__(self):
        return await self.reqco

    async def __aexit__(self, *args):
        await _aclose(self.client._reader, self.client._writer)
        return await asyncio.sleep(0)


# ===========================================================================
# HTTP client (was aiohttp/__init__.py)
# ===========================================================================

HttpVersion10 = 'HTTP/1.0'
HttpVersion11 = 'HTTP/1.1'

# Sentinel: a per-request `timeout` left at this value means "use the session
# default". Passing timeout=None explicitly means "no timeout" (wait forever).
_UNSET = object()


def _with_timeout(aw, timeout):
    # asyncio.wait_for exists on both MicroPython (>=1.13) and CPython, and a
    # None timeout short-circuits to a plain await on both -- so this is free
    # when no timeout is configured. On expiry it raises asyncio.TimeoutError.
    return asyncio.wait_for(aw, timeout)


class ClientResponse:
    def __init__(self, reader):
        self.content = reader
        # Keep-alive bookkeeping: a connection may only be reused once the whole
        # body has been read off the socket, so the next request starts clean.
        self._read = 0
        self._content_length = None
        # Timeout (seconds, or None) applied to body reads, and a back-reference
        # to the session so a timed-out read can drop the now-unusable socket.
        self._timeout = None
        self._session = None

    def _get_header(self, keyname, default):
        for k in self.headers:
            if k.lower() == keyname:
                return self.headers[k]
        return default

    def _decode(self, data):
        c_encoding = self._get_header('content-encoding', None)
        if c_encoding not in ('gzip', 'deflate', 'gzip,deflate'):
            return data
        # MicroPython ships the `deflate` module; on CPython the import fails and
        # we fall back to stdlib `zlib`. The ImportError is the runtime switch.
        try:
            import deflate
            import io

            if c_encoding == 'deflate':
                with deflate.DeflateIO(io.BytesIO(data), deflate.ZLIB) as d:
                    return d.read()
            elif c_encoding == 'gzip':
                with deflate.DeflateIO(io.BytesIO(data), deflate.GZIP, 15) as d:
                    return d.read()
        except ImportError:
            import zlib

            if c_encoding == 'deflate':
                return zlib.decompress(data)  # zlib-wrapped (matches deflate.ZLIB)
            elif c_encoding == 'gzip':
                return zlib.decompress(data, 16 + zlib.MAX_WBITS)  # gzip wrapper
        return data

    async def _drain_read(self, sz):
        # MicroPython fast path: read exactly `sz` bytes by draining the socket greedily,
        # only sleeping when it genuinely has nothing (EAGAIN / None), instead of yielding to
        # the asyncio poller per read like StreamReader.readexactly().
        #
        # Why: readexactly() does `yield core._io_queue.queue_read(s)` before every read, which
        # waits for the underlying TCP fd to be poll-readable. Over TLS, mbedTLS buffers a whole
        # decrypted record internally; once it has drained the kernel buffer the fd reads "not
        # readable" even though bytes are ready, so the poll stalls until the next segment. On a
        # slow link that is ~1 record per wakeup -- orders of magnitude slower than the link.
        # Draining recovers it (measured ~12x faster body reads over TLS on ESP32).
        #
        # `self.content.s` is the MicroPython asyncio Stream's underlying socket; bytes left in
        # it by header parsing (readline) stay in the same object, so nothing is skipped. A short
        # read (socket error / EOF) just returns what it has -- callers validate length/CRC.
        sock = self.content.s
        chunks = []
        remaining = sz
        while remaining > 0:
            try:
                c = sock.read(remaining)
            except OSError as e:
                if e.args[0] in _BUSY_ERRORS:
                    await _sleep_ms(10)
                    continue
                break  # real socket error -> stop; short read surfaced to the caller
            if c is None:
                await _sleep_ms(10)  # nothing available yet
                continue
            if c == b'':
                break  # EOF
            chunks.append(c)
            remaining -= len(c)
        return b''.join(chunks)

    async def read(self, sz=-1):
        # On a keep-alive connection the server never closes the socket, so a bare
        # read(-1) (read-to-EOF) would block forever. When Content-Length is known,
        # bound the default read to the bytes still outstanding.
        if sz == -1 and self._content_length is not None:
            sz = self._content_length - self._read
        try:
            if _IS_MICROPYTHON and sz != -1:
                # Known-length body on MicroPython: use the greedy drain (see _drain_read).
                data = await _with_timeout(self._drain_read(sz), self._timeout)
            else:
                data = await _with_timeout(
                    self.content.read(sz) if sz == -1 else self.content.readexactly(sz),
                    self._timeout,
                )
        except asyncio.TimeoutError:
            # A stalled body read leaves the socket mid-response; drop it so the
            # next request on this session can't inherit the garbage.
            if self._session is not None:
                await self._session._close_conn()
            raise
        self._read += len(data)  # bytes pulled off the socket (pre-decode) for keep-alive
        return self._decode(data)

    async def text(self, encoding='utf-8'):
        return (await self.read(int(self._get_header('content-length', -1)))).decode(encoding)

    async def json(self):
        return _json.loads(await self.read(int(self._get_header('content-length', -1))))

    def __repr__(self):
        return '<ClientResponse %d %s>' % (self.status, self.headers)


class ChunkedClientResponse(ClientResponse):
    def __init__(self, reader):
        self.content = reader
        self.chunk_size = 0
        # Chunked bodies have no Content-Length, so _content_length stays None and
        # the connection is never reused (closed on exit) — safe, simple default.
        self._read = 0
        self._content_length = None
        self._timeout = None
        self._session = None

    async def read(self, sz=4 * 1024 * 1024):
        try:
            if self.chunk_size == 0:
                l = await _with_timeout(self.content.readline(), self._timeout)
                l = l.split(b';', 1)[0]
                self.chunk_size = int(l, 16)
                if self.chunk_size == 0:
                    # End of message
                    sep = await _with_timeout(self.content.readexactly(2), self._timeout)
                    assert sep == b'\r\n'
                    return b''
            want = min(sz, self.chunk_size)
            # Chunk DATA read: on MicroPython use the greedy drain (inherited from
            # ClientResponse) -- the chunked equivalent of the Content-Length fast path. The
            # tiny chunk-size line and trailing CRLF stay on the builtin reader (a few bytes).
            if _IS_MICROPYTHON:
                data = await _with_timeout(self._drain_read(want), self._timeout)
            else:
                data = await _with_timeout(self.content.readexactly(want), self._timeout)
            self.chunk_size -= len(data)
            if self.chunk_size == 0:
                sep = await _with_timeout(self.content.readexactly(2), self._timeout)
                assert sep == b'\r\n'
        except asyncio.TimeoutError:
            if self._session is not None:
                await self._session._close_conn()
            raise
        return self._decode(data)

    def __repr__(self):
        return '<ChunkedClientResponse %d %s>' % (self.status, self.headers)


class _RequestContextManager:
    def __init__(self, client, request_co):
        self.reqco = request_co
        self.client = client
        self.resp = None

    async def __aenter__(self):
        self.resp = await self.reqco
        return self.resp

    async def __aexit__(self, *args):
        # Reuse the connection (HTTP keep-alive) only when the server agreed to keep
        # it open AND the whole body was consumed off the socket. Otherwise close it,
        # so a partially-read or close-marked response can never corrupt the next one.
        resp = self.resp
        fully_read = (
            resp is not None
            and resp._content_length is not None
            and resp._read >= resp._content_length
        )
        if self.client._reuse and fully_read:
            pass  # leave self.client._reader/_writer live for the next request
        else:
            await self.client._close_conn()
        return await asyncio.sleep(0)


class ClientSession:
    def __init__(self, base_url='', headers={}, version=HttpVersion11, timeout=None):
        self._reader = None
        self._writer = None
        self._conn_key = None  # (host, port, bool(ssl)) of the currently-open socket
        self._reuse = False  # True when the open socket may be reused (keep-alive)
        self.last_reused = False  # instrumentation: did the last request reuse the socket?
        # Default per-operation timeout in seconds (None = wait forever). Applied to
        # connect, send, and every read; a single stalled op past this aborts the
        # request with asyncio.TimeoutError. Essential when polling LAN devices that
        # may be offline (connect hangs) or wedged (read hangs). Override per call
        # with request(..., timeout=...).
        self._timeout = timeout
        self._base_url = base_url
        # HTTP/1.1 + keep-alive so all requests to the same host share ONE TCP/TLS
        # connection. On ESP32 the per-request TLS handshake dominated OTA time, so
        # reusing the connection is the big win (see fota_fwu in mqtt.py).
        self._base_headers = {'Connection': 'keep-alive', 'User-Agent': 'compat'}
        self._base_headers.update(**headers)
        self._http_version = version

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self._close_conn()
        return await asyncio.sleep(0)

    async def _close_conn(self):
        """Close and forget the persistent connection (if any)."""
        if self._reader is not None or self._writer is not None:
            await _aclose(self._reader, self._writer)
        self._reader = None
        self._writer = None
        self._conn_key = None
        self._reuse = False

    async def _request(
        self, method, url, data=None, json=None, ssl=None, params=None, headers={}, timeout=None
    ):
        try:
            return await self._request_impl(
                method, url, data, json, ssl, params, headers, timeout
            )
        except asyncio.TimeoutError:
            # Connect/send/header-read timed out: the socket (if any) is in an
            # indeterminate state, so drop it before the error propagates.
            await self._close_conn()
            raise

    async def _request_impl(self, method, url, data, json, ssl, params, headers, timeout):
        redir_cnt = 0
        stale_retry = True
        while redir_cnt < 2:
            reader = await self.request_raw(
                method, url, data, json, ssl, params, headers, timeout=timeout
            )
            _headers = []
            sline = await _with_timeout(reader.readline(), timeout)
            if not sline:
                # Empty status line = the reused keep-alive socket was closed by the
                # peer between requests. Drop it and retry once on a fresh connection.
                await self._close_conn()
                if stale_retry:
                    stale_retry = False
                    _log('[HTTP] keep-alive socket stale (closed by peer) — reconnecting')
                    continue
                raise OSError('connection closed by peer')
            sline = sline.split(None, 2)
            http_ver = sline[0]
            status = int(sline[1])
            chunked = False
            conn_close = False
            while True:
                line = await _with_timeout(reader.readline(), timeout)
                if not line or line == b'\r\n':
                    break
                _headers.append(line)
                # Header names are case-insensitive (RFC 7230); some servers
                # (e.g. go-httpbin, HTTP/2 proxies) send them lowercased.
                lname = line.lower()
                if lname.startswith(b'transfer-encoding:'):
                    if b'chunked' in lname:
                        chunked = True
                elif lname.startswith(b'connection:'):
                    if b'close' in lname:
                        conn_close = True
                elif lname.startswith(b'location:'):
                    url = line.rstrip().split(None, 1)[1].decode()

            if 301 <= status <= 303:
                redir_cnt += 1
                await self._close_conn()
                continue
            break

        # The socket may be reused only for HTTP/1.1 responses the server didn't mark
        # `Connection: close`. The _RequestContextManager additionally requires the body
        # to be fully read before it actually keeps the connection open.
        self._reuse = (http_ver == b'HTTP/1.1') and not conn_close

        if chunked:
            resp = ChunkedClientResponse(reader)
        else:
            resp = ClientResponse(reader)
        # Body reads inherit the request timeout; the back-reference lets a stalled
        # read drop this session's socket.
        resp._timeout = timeout
        resp._session = self
        resp.status = status
        resp.headers = _headers
        resp.url = url
        if params:
            resp.url += '?' + '&'.join(f'{k}={params[k]}' for k in sorted(params))
        try:
            resp.headers = {
                val.split(':', 1)[0]: val.split(':', 1)[-1].strip()
                for val in [hed.decode().strip() for hed in _headers]
            }
        except Exception:
            pass
        # Record the declared body size so the context manager knows when the socket
        # has been fully drained and is safe to reuse (keep-alive).
        # HEAD and 204/304 responses carry NO body even though they may still send a
        # Content-Length (the size a GET would return). Treating that header as a body
        # length would make read() block forever on bytes that never arrive, so pin
        # the body to empty for those.
        if method == 'HEAD' or status in (204, 304):
            resp._content_length = 0
        else:
            cl = resp._get_header('content-length', None)
            if cl is not None:
                try:
                    resp._content_length = int(cl)
                except (ValueError, TypeError):
                    resp._content_length = None
        self._reader = reader
        return resp

    async def request_raw(
        self,
        method,
        url,
        data=None,
        json=None,
        ssl=None,
        params=None,
        headers={},
        is_handshake=False,
        version=None,
        timeout=None,
    ):
        if json and isinstance(json, dict):
            data = _json.dumps(json)
        if data is not None and method == 'GET':
            method = 'POST'
        if params:
            url += '?' + '&'.join(f'{k}={params[k]}' for k in sorted(params))
        try:
            proto, dummy, host, path = url.split('/', 3)
        except ValueError:
            proto, dummy, host = url.split('/', 2)
            path = ''

        if proto == 'http:':
            port = 80
        elif proto == 'https:':
            port = 443
            if ssl is None:
                ssl = True
        else:
            raise ValueError('Unsupported protocol: ' + proto)

        if ':' in host:
            host, port = host.split(':', 1)
            port = int(port)

        # Keep-alive: reuse the live socket when it targets the same host:port:ssl.
        # Handshakes (WebSocket) always get their own dedicated connection.
        conn_key = (host, port, bool(ssl))
        if self._reuse and self._conn_key == conn_key and self._writer is not None and not is_handshake:
            reader, writer = self._reader, self._writer
            reused = True
        else:
            await self._close_conn()
            # Resolve DNS separately first so a slow/failing lookup is visible on its own
            # (open_connection lumps DNS + TCP + TLS into one timing). getaddrinfo is cached,
            # so the resolve inside open_connection right after is cheap. Logged only via the
            # optional set_logger() hook (no-op by default).
            t_dns = 0
            ip = '?'
            try:
                _td = ticks_ms()
                ai = socket.getaddrinfo(host, port)
                t_dns = ticks_diff(ticks_ms(), _td)
                ip = ai[0][-1][0] if ai else '?'
            except Exception as e:
                ip = 'dns-err {}: {}'.format(type(e).__name__, e)
            _log('[HTTP] connecting {}:{} ssl={} dns={}ms -> {}'.format(host, port, bool(ssl), t_dns, ip))
            _tc = ticks_ms()
            reader, writer = await _with_timeout(
                asyncio.open_connection(host, port, ssl=ssl), timeout
            )
            _log('[HTTP] connected {}:{} in {}ms (tcp+tls)'.format(host, port, ticks_diff(ticks_ms(), _tc)))
            self._conn_key = conn_key
            reused = False
        self.last_reused = reused  # instrumentation

        if version is None:
            version = self._http_version
        if 'Host' not in headers:
            headers.update(Host=host)
        # Build the request as str then encode(), so the same code produces
        # identical bytes on MicroPython and CPython. (CPython's bytes-%
        # formatting rejects str operands; MicroPython's is lenient.)
        if not data:
            hdrs = ('\r\n'.join(f'{k}: {v}' for k, v in headers.items()) + '\r\n') if headers else ''
            query = ('%s /%s %s\r\n%s\r\n' % (method, path, version, hdrs)).encode()
        else:
            if json:
                headers.update(**{'Content-Type': 'application/json'})
            if isinstance(data, bytes):
                headers.update(**{'Content-Type': 'application/octet-stream'})
            else:
                data = data.encode()

            headers.update(**{'Content-Length': len(data)})
            hdrs = '\r\n'.join(f'{k}: {v}' for k, v in headers.items()) + '\r\n'
            query = ('%s /%s %s\r\n%s\r\n' % (method, path, version, hdrs)).encode() + data

        # write()/drain() is supported by both MicroPython and CPython asyncio.
        try:
            writer.write(query)
            await _with_timeout(writer.drain(), timeout)
        except (OSError, ConnectionError):
            # A reused keep-alive socket died on write — reconnect once and resend.
            if not reused:
                raise
            await self._close_conn()
            reader, writer = await _with_timeout(
                asyncio.open_connection(host, port, ssl=ssl), timeout
            )
            self._conn_key = conn_key
            self.last_reused = False  # instrumentation: stale socket forced a reconnect
            writer.write(query)
            await _with_timeout(writer.drain(), timeout)
        self._reader = reader
        self._writer = writer
        if not is_handshake:
            return reader
        else:
            return reader, writer

    def request(
        self, method, url, data=None, json=None, ssl=None, params=None, headers={}, timeout=_UNSET
    ):
        # Merge base + per-request headers without `dict(**a, **b)`: multiple **
        # unpacking in one call is a SyntaxError on MicroPython 1.18.
        merged = dict(self._base_headers)
        merged.update(headers)
        # timeout left as _UNSET -> use the session default; an explicit value
        # (including None for "no timeout") overrides it for this request only.
        eff_timeout = self._timeout if timeout is _UNSET else timeout
        return _RequestContextManager(
            self,
            self._request(
                method,
                self._base_url + url,
                data=data,
                json=json,
                ssl=ssl,
                params=params,
                headers=merged,
                timeout=eff_timeout,
            ),
        )

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        return self.request('PUT', url, **kwargs)

    def patch(self, url, **kwargs):
        return self.request('PATCH', url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request('DELETE', url, **kwargs)

    def head(self, url, **kwargs):
        return self.request('HEAD', url, **kwargs)

    def options(self, url, **kwargs):
        return self.request('OPTIONS', url, **kwargs)

    def ws_connect(self, url, ssl=None):
        return _WSRequestContextManager(self, self._ws_connect(url, ssl=ssl))

    async def _ws_connect(self, url, ssl=None):
        ws_client = WebSocketClient(self._base_headers.copy())
        await ws_client.connect(url, ssl=ssl, handshake_request=self.request_raw)
        self._reader = ws_client.reader
        return ClientWebSocketResponse(ws_client)
