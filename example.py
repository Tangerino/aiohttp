# Usage examples for the single-module aiohttp client.
# MIT license; Copyright (c) 2026 Carlos Tangerino
#
# Runs unchanged on CPython and MicroPython:
#
#   $ python example.py                                  # PC (CPython)
#   $ mpremote connect PORT mount . run example.py       # board (needs WiFi)
#
# On a board, bring up WiFi before the requests run -- uncomment the connect_wifi
# block in main() and set your SSID/password. On a PC the host network is used and
# no setup is needed.

import asyncio

import aiohttp

BASE = 'https://postman-echo.com'
WS_URL = 'wss://ws.postman-echo.com/raw'


async def simple_get(session):
    # GET a URL and read the JSON body. The response is an async context manager,
    # so the connection is released (or kept alive) automatically on exit.
    async with session.get(BASE + '/get') as resp:
        print('GET /get ->', resp.status)
        data = await resp.json()
        print('  echoed url:', data['url'])


async def get_with_params(session):
    # `params` are appended as a sorted query string.
    async with session.get(BASE + '/get', params={'q': 'micropython', 'page': '2'}) as resp:
        print('GET with params ->', resp.status)
        print('  args:', (await resp.json())['args'])


async def post_json(session):
    # `json=` serializes the body and sets Content-Type: application/json.
    async with session.post(BASE + '/post', json={'hello': 'world'}) as resp:
        print('POST json ->', resp.status)
        print('  server saw:', (await resp.json())['json'])


async def post_text(session):
    # `data=` sends a raw body (str here; bytes -> application/octet-stream).
    async with session.post(BASE + '/post', data='plain text body') as resp:
        print('POST data ->', resp.status)
        print('  server saw:', (await resp.json())['data'])


async def custom_headers(session):
    async with session.get(BASE + '/headers', headers={'X-Demo': 'aiohttp'}) as resp:
        print('GET /headers ->', resp.status)
        echoed = (await resp.json())['headers']
        print('  X-Demo echoed:', echoed.get('x-demo'))


async def with_timeout(session):
    # Per-request timeout (seconds) for every network wait. A wedged/offline host
    # aborts with asyncio.TimeoutError instead of hanging forever. /delay/5 stalls
    # the response 5s; a 1s timeout fires first.
    try:
        async with session.get(BASE + '/delay/5', timeout=1) as resp:
            await resp.read()
        print('timeout demo -> (unexpected) completed')
    except asyncio.TimeoutError:
        print('timeout demo -> aborted after ~1s, as expected')


async def websocket_echo(session):
    # WebSocket over the same session: send/receive, or iterate frames.
    async with session.ws_connect(WS_URL) as ws:
        await ws.send_str('ping')
        print('WS ->', await ws.receive_str())

        await ws.send_json({'a': 1})
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                print('WS iter ->', msg.data)
                break


async def main():
    # --- On a board, bring up WiFi first (no-op / skip on a PC): ---------------
    # import network
    # wlan = network.WLAN(network.STA_IF)
    # wlan.active(True)
    # wlan.connect('your-ssid', 'your-password')
    # while not wlan.isconnected():
    #     await asyncio.sleep(0.2)
    # --------------------------------------------------------------------------

    # One session, reused across requests. With HTTP/1.1 keep-alive the requests
    # to the same host share a single TCP/TLS connection. A 10s default timeout
    # guards every call unless overridden per request.
    async with aiohttp.ClientSession(timeout=10) as session:
        await simple_get(session)
        await get_with_params(session)
        await post_json(session)
        await post_text(session)
        await custom_headers(session)
        await with_timeout(session)
        await websocket_echo(session)

    print('done.')


asyncio.run(main())
