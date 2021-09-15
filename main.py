from dotenv import load_dotenv
import os
from Streamer import Streamer


async def subscribe_orderbook(self, orderbook_update):
    self.logger.info("orderbook_update: {ou}".format(ou=orderbook_update))
    print('quote: ', orderbook_update)


def main():
    # Read these from your .env file, and add it to your .gitignore file!
    load_dotenv()
    # print(os.environ.get("key_id"))
    # print(os.environ.get("secret_key"))
    key_id = os.environ.get('key_id')
    secret_key = os.environ.get('secret_key')
    phemex_testnet_ws_base_url = "wss://testnet.phemex.com/ws"

    streamer = Streamer(key_id=key_id, secret_key=secret_key, base_url=phemex_testnet_ws_base_url)
    streamer.subscribe_orderbook(subscribe_orderbook, "ETHUSD")
    streamer.run()


if __name__ == '__main__':
    main()
