import hmac
import spdlog as spd
from datetime import date
import time
from math import trunc
import websockets
import asyncio
import queue
import hashlib
import os


def ensure_coroutine(handler):
    if not asyncio.iscoroutinefunction(handler):
        raise ValueError('handler must be a coroutine function')


class Streamer:
    def __init__(self, key_id: str, secret_key: str, base_url: str):
        self.key_id_ = key_id
        self.secret_key_ = secret_key
        self.base_url_ = base_url
        self.ws_ = None
        self.running_ = False
        self.stop_stream_queue_ = queue.Queue()

        self.trade_handlers_ = {}
        self.orderbook_handlers_ = {}

        # Create logger
        today_date = date.today().strftime("%b-%d-%Y")

        path = "./log"
        # Check whether the specified path exists or not
        path_exists = os.path.exists(path)

        if not path_exists:
            # If log directory does not exists, create log directory
            os.makedirs(path)

        self.logger = spd.FileLogger("fast_logger", "./log/{}.log".format(today_date))
        self.logger.set_level(spd.LogLevel.INFO)

    def __del__(self):
        self.ws_ = None
        self.logger.info("Destructor called, Streamer deleted.")

    async def connect(self):
        try:
            self.ws_ = await websockets.connect(self.base_url_)
        except self.ws_ is None:
            self.logger.error("Can't connect to websocket: {ws_base_url}".format(ws_base_url=self.base_url_))
        self.logger.info("connected to: {base_url_}".format(base_url_=self.base_url_))

    async def auth(self):
        # Expiry time for request
        expiry_time = trunc(time.time()) + 120

        # Create a hash with secret key and SHA256
        digest = hmac.new(key=str.encode(self.secret_key_), msg=str.encode(self.key_id_ + str(expiry_time)),
                          digestmod=hashlib.sha256)
        signature = digest.hexdigest()

        api_auth = "{\"method\": \"user.auth\", \"params\": [\"API\", \"" + \
                   self.key_id_ + "\", \"" + signature + "\", " + str(expiry_time) + "], \"id\": 1234}"
        await self.ws_.send(api_auth)
        self.logger.info("Sending websocket authentication attempt.")

    async def start_ws(self):
        await self.connect()
        await self.auth()
        await asyncio.sleep(delay=0.2)  # Wait for authentication response
        await self.subscribe_all()

    async def dispatch(self, msg):
        # For now it is just logging any message received.
        self.logger.info(msg)
        # TODO Use handler functions to handle specific messages, after parsing them for specifics, ideally,
        #  handlers would then serialize the message into an object. Look at commented out code below as a guideline.

        # handler = self.orderbook_handlers_.get(symbol, self.orderbook_handlers_.get('*', None))
        # if handler:
        #     await handler(msg)
        # msg_type = msg.get('T')
        # symbol = msg.get('S')
        # if msg_type == 't':
        #     handler = self.trade_handlers_.get(
        #         symbol, self.trade_handlers_.get('*', None))
        #     if handler:
        #         await handler(self._cast(msg_type, msg))
        # elif msg_type == 'q':
        #     handler = self._quote_handlers.get(
        #         symbol, self._quote_handlers.get('*', None))
        #     if handler:
        #         await handler(self._cast(msg_type, msg))
        # elif msg_type == 'subscription':
        #     log.info(f'subscribed to trades: {msg.get("trades", [])}, ' +
        #              f'quotes: {msg.get("quotes", [])} ' +
        #              f'bars: {msg.get("bars", [])}, ' +
        #              f'daily bars: {msg.get("dailyBars", [])}, ' +
        #              f'statuses: {msg.get("statuses", [])}'
        #              )
        # elif msg_type == 'error':
        #     log.error(f'error: {msg.get("msg")} ({msg.get("code")})')

    async def consume(self):
        while True:
            if not self.stop_stream_queue_.empty():
                self.stop_stream_queue_.get()
                break
            else:
                msg = await self.ws_.recv()
                await self.dispatch(msg)

    async def run_forever(self):
        # Do not start the websocket connection until we subscribe to something
        while not (self.trade_handlers_ or self.orderbook_handlers_):
            if not self.stop_stream_queue_.empty():
                self.stop_stream_queue_.get()
                return
            await asyncio.sleep(0.1)

        retries = 0
        self.running_ = False

        while True:
            try:
                if not self.running_:
                    await self.start_ws()
                    self.logger.info('Started streaming')
                    self.running_ = True
                    retries = 0
                await self.consume()
            except websockets.WebSocketException as wse:
                retries += 1
                if retries > 3:
                    self.logger.info("Max retries exceeded, signalling to close the websocket.")
                    await self.stop_ws()
                    raise ConnectionError("max retries exceeded")
                if retries > 1:
                    await asyncio.sleep(3)
                self.logger.warn('Websocket error, restarting connection: ' + str(wse))
            finally:
                if not self.running_:
                    await self.ws_.close()
                    self.logger.info("Websocket has been gracefully shutdown.")
                    break
                await asyncio.sleep(0.01)

    async def stop_ws(self):
        # This signals to stop the websocket
        self.logger.info("Proceeding to signal to shutdown the socket")
        self.stop_stream_queue_.put_nowait({"should_stop": True})
        self.running_ = False
        await self.ws_.close()

    def run(self):
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.run_forever())
        except KeyboardInterrupt:
            # This signals to stop the websocket
            self.logger.info("Mouse or keyboard interrupt, proceeding to signal to shutdown the socket.")
            self.stop_stream_queue_.put_nowait({"should_stop": True})
            self.running_ = False

    async def subscribe_all(self):
        if self.trade_handlers_ or self.orderbook_handlers_:
            symbols = ""
            for symbol in self.orderbook_handlers_.keys():
                symbols += "\"" + symbol + "\" ,"

            symbols = symbols[:-1]  # Remove last comma from symbols string
            await self.ws_.send(
                "{\"id\":" + "1234" + ", \"method\": \"orderbook.subscribe\", \"params\": [" + symbols + "]}")

    def subscribe(self, handler, symbols, handlers):
        ensure_coroutine(handler)
        for symbol in symbols:
            handlers[symbol] = handler
        if self.running_:
            asyncio.get_event_loop().run_until_complete(self.subscribe_all())

    def subscribe_orderbook(self, handler, *symbols):
        self.subscribe(handler, symbols, self.orderbook_handlers_)
