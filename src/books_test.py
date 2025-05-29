
import asyncio

from okx.websocket.WsPublicAsync import WsPublicAsync


def callbackFunc(message):
    print(message)


async def main():
    ws = WsPublicAsync(url="wss://ws.okx.com:8443/ws/v5/public")
    await ws.start()
    args = [
      {
        "channel": "books",
        "instId": "BTC-USDT"
      }
    ]

    await ws.subscribe(args, callback=callbackFunc)
    await asyncio.sleep(10000)

    # await ws.unsubscribe(args, callback=callbackFunc)
    # await asyncio.sleep(10)

asyncio.run(main())
