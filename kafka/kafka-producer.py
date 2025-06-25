import yfinance as yf
from confluent_kafka import Producer
import json
import time

# Kafka config
producer = Producer({'bootstrap.servers': 'localhost:9092'})
topic = 'yahoo-finance-topic'

# List of stock symbols to track
symbols = ['SNOW', 'AMZN', 'GOOGL', 'MSFT']

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Published to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

while True:
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        data = stock.info  # full info; can also use `stock.history()` for OHLC
        
        message = {
            'symbol': symbol,
            'price': data.get('regularMarketPrice'),
            'currency': data.get('currency'),
            'time': time.strftime('%Y-%m-%d %H:%M:%S'),
        }

        producer.produce(topic, value=json.dumps(message), key=symbol, callback=acked)
    
    producer.flush()
    time.sleep(30)  # Fetch every 30 seconds
