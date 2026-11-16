"""Coinbase trades -> Kafka topic "coinbase_trades".

  python producer.py        # PUSH: live trades from the Coinbase WebSocket (Ctrl+C to stop)

Adapted from the class script quakes_producer.py (Mon 02 Nov). The WebSocket
connection follows probe.py and Appendix A of the Session 1 worksheet.
Coinbase has no history on this feed, so there is only a live mode.
"""
import asyncio
import json
import logging

from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "coinbase_trades"
WSS_URL = "wss://ws-feed.exchange.coinbase.com"
SUBSCRIBE = {
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD"],
    "channels": ["matches"],  # one message per trade
}
LOG_EVERY = 1000   # fast source: one log line per 1,000 messages, not per message
RETRY_SECONDS = 5  # pause before reconnecting after a dropped connection

sent = 0


def send(producer, message):
    """One Kafka record per message. Key = product_id, so every trade of one
    product goes to the same partition, in order. Envelope as in class."""
    global sent
    key = message.get("product_id")  # .get(): no crash if the field is missing
    if key is None:  # e.g. the 'subscriptions' message
        logging.warning("No product_id (type=%s): sent without a key", message.get("type"))
    producer.produce(topic=TOPIC, key=key, value=json.dumps({"action": "live", "data": message}))
    sent += 1
    if sent % LOG_EVERY == 0:
        logging.info("%d messages sent to %s", sent, TOPIC)


async def live(producer):
    """PUSH over a WebSocket, with a reconnect loop and a pause."""
    while True:
        try:
            ws = await websocket_connect(WSS_URL, ping_interval=15)
            await ws.write_message(json.dumps(SUBSCRIBE))  # Coinbase closes the connection after 5 s without it
            logging.info("Connected and subscribed to %s", ", ".join(SUBSCRIBE["product_ids"]))
            while True:
                text = await ws.read_message()
                if text is None:
                    logging.warning("The server closed the connection")
                    break
                try:
                    send(producer, json.loads(text))
                except (ValueError, TypeError, AttributeError) as e:
                    logging.warning("Skipped a message I could not read: %r", e)
        except Exception as e:  # DNS error, Wi-Fi off, refused connection...
            logging.warning("Connection failed: %r", e)
        logging.info("Reconnecting in %d s (sent so far: %d)", RETRY_SECONDS, sent)
        await asyncio.sleep(RETRY_SECONDS)


def main():
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Stopped. %d messages sent to %s", sent, TOPIC)
