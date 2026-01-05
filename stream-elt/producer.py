# producer.py
from dotenv import load_dotenv
load_dotenv()


import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,             # Log level (INFO, DEBUG, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/producer.log'),  # Log to both tmp file and console
        logging.StreamHandler()
    ]
)

logging.info("Producer started")

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

import os, json, time, math, uuid
from typing import Any, Dict, List
from websocket import WebSocketApp
from confluent_kafka import Producer


# ----- Config from env -----
API_KEY_ID = os.getenv("APCA_API_KEY_ID")
API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

BOOTSTRAPS = os.getenv("CCLOUD_BOOTSTRAP_SERVERS")
CCLOUD_API_KEY = os.getenv("CCLOUD_API_KEY")
CCLOUD_API_SECRET = os.getenv("CCLOUD_API_SECRET")
TOPIC = os.getenv("TOPIC_NAME", "market_bars")


SR_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_API_KEY = os.getenv("SR_API_KEY")
SR_API_SECRET = os.getenv("SR_API_SECRET")

sr_conf = {
    "url": SR_URL,
    "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
}

sr_client = SchemaRegistryClient(sr_conf)

# market_bar topic avro schema 
market_bar_avsc = r"""
{
  "type": "record",
  "namespace": "com.capstone.marketdata",
  "name": "MarketBar",
  "doc": "One-minute bar message for US equities.",
  "fields": [
    {"name":"schema_version","type":"int","default":1},
    {"name":"event_type","type":{"type":"enum","name":"EventType","symbols":["BAR"]}},
    {"name":"bar_interval","type":{"type":"enum","name":"BarInterval","symbols":["ONE_MIN"]}},
    {"name":"symbol","type":"string"},
    {"name":"open","type":"double"},
    {"name":"high","type":"double"},
    {"name":"low","type":"double"},
    {"name":"close","type":"double"},
    {"name":"volume","type":"long"},
    {"name":"vwap","type":["null","double"],"default":null},
    {"name":"trade_count","type":"int"},
    {"name":"ts_exchange","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"ts_event_ms","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"source","type":"string"}
  ]
}
"""

# Choose your Alpaca feed:
# - "v2/iex" (real-time IEX) -> bars during reg trading hours
# - "v2/delayed_sip" (15-min delayed SIP)
# - "v2/test" (always-on, symbol FAKEPACA)
FEED = "v2/iex"
BASE_URL = f"wss://stream.data.alpaca.markets/{FEED}"



# ----- Confluent Cloud Producer -----
def to_avro(obj: dict, ctx: SerializationContext) -> dict:
    """
    AvroSerializer expects a dict whose fields match the schema.
    Return the same dict; normalization is handled by the serializer.
    """
    return obj

# adding a key (obj) serializer for bytes
# Kafka keys are transmitted as bytes 
def bytes_serializer(obj, ctx):
    """
    SerializingProducer key.serializer must return bytes.
    If obj is already bytes, return as-is.
    """
    if obj is None:
        return None
    if isinstance(obj, (bytes, bytearray)):
        return bytes(obj)
    # if someone passes uuid.UUID, support it
    if isinstance(obj, uuid.UUID):
        return obj.bytes

avro_serializer = AvroSerializer(
    schema_str=market_bar_avsc,
    schema_registry_client=sr_client,
    to_dict=to_avro
)

producer_conf = {
    "bootstrap.servers": BOOTSTRAPS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": CCLOUD_API_KEY,
    "sasl.password": CCLOUD_API_SECRET,
    "client.id": "alpaca-bars-producer",
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "lz4",
    "linger.ms": 50,
    "message.timeout.ms": 30000,
    "key.serializer": bytes_serializer,
    "value.serializer": avro_serializer
}
producer = SerializingProducer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        pass

# seconds until the next :00 minute mark, plus 3 seconds
def seconds_to_next_minute(now: float) -> float:
    frac = now - math.floor(now/60)*60
    return (60 - frac) + 3.0  # 3s delay past the minute mark; ensures prior minute's data is complete

def publish_bar(bar: Dict[str, Any], source: str):
    """
    bar dict includes: symbol, open, high, low, close, volume, vwap, trade_count, timestamp
    """

    payload = {
        "schema_version": 1,
        "event_type": "BAR",
        "bar_interval": "ONE_MIN",
        "symbol": bar["symbol"],
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": float(bar["close"]),
        "volume": int(bar["volume"]),
        "vwap": bar.get("vwap"),                             # None is OK (union ["null","double"])
        "trade_count": int(bar.get("trade_count", 0)),       # ensure non-null int
        "ts_exchange": bar["timestamp"],                     # RFC-3339 string (Alpaca)
        "ts_event_ms": int(time.time() * 1000),              # Avro logical timestamp-millis
        "source": source
    }

    key = uuid.uuid4().bytes   # 16-byte VARBINARY key

    while True:
        try:
            producer.produce(
                topic=TOPIC,
                key=key,
                value=payload,
                on_delivery=delivery_report
            )
            producer.poll(0)
            break
        except BufferError:
            producer.poll(0.1)

def stream_bars(symbols: List[str]):
    if not symbols:
        raise ValueError("symbols must not be empty.")
    if not API_KEY_ID or not API_SECRET_KEY:
        raise ValueError("Missing APCA_API_KEY_ID / APCA_API_SECRET_KEY.")
    pairs = [s.upper() for s in symbols]

    start_ts = time.time()
    MAX_WAIT_SECONDS = min(90, seconds_to_next_minute(start_ts))  

    def on_open(ws):
        # Authenticate (Alpaca requires auth within ~10s of connect)
        ws.send(json.dumps({"action": "auth", "key": API_KEY_ID, "secret": API_SECRET_KEY}))
        ws.send(json.dumps({"action": "subscribe", "bars": pairs}))

    def on_message(ws, message):
        try:
            data = json.loads(message)
        except Exception:
            print("Non-JSON message:", message); return
        events = data if isinstance(data, list) else [data]
        for evt in events:
            T = evt.get("T")
            if T == "success":
                print("SUCCESS:", evt.get("msg"))
            elif T == "subscription":
                print("SUBSCRIBED:", evt)
            elif T == "error":
                print("SERVER ERROR:", evt)
            elif T == "b":
                # Minute bar: fields o/h/l/c/v/vw/n/t  (symbol S)
                sym = evt.get("S")
                if not sym: continue
                bar = {
                    "symbol": sym,
                    "open": evt.get("o"),
                    "high": evt.get("h"),
                    "low": evt.get("l"),
                    "close": evt.get("c"),
                    "volume": evt.get("v"),
                    "vwap": evt.get("vw"),
                    "trade_count": evt.get("n"),
                    "timestamp": evt.get("t"),
                }
                publish_bar(bar, source=f"alpaca:{FEED}")

        # keep connection open; do not close after first bar
        # Only do this timeout logic if the selected feed is v2/test
        if time.time() - start_ts >= MAX_WAIT_SECONDS and FEED == "v2/test":
            ws.close()

    def on_error(ws, err):
        print("WS error:", err)

    def on_close(ws, code, reason):
        print("WS closed:", code, reason)

    ws = WebSocketApp(BASE_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()

if __name__ == "__main__":
    # real-time IEX during reg trading hours
    tickers = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'GOOG', 'META', 'TSLA', 'NVDA']

    # For 24/7 testing, switch feed:
    # FEED = "v2/test"; BASE_URL = f"wss://stream.data.alpaca.markets/{FEED}"; tickers = ['FAKEPACA']

    try:
        stream_bars(tickers)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        # Flush outstanding delivery reports
        producer.flush(10_000)

