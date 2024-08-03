from kafka import KafkaProducer
import json
import binascii
import datetime
import threading 
import time
import random


input_data = [
    {"symbol": "IBM", "price": 108.4, "change": 0.2},
    {"symbol": "MSFT", "price": 10.3,  "change": 0.2},
    {"symbol": "GOOG", "price": 1002.3,  "change": 0.2}\
]


def run():
    LIMIT = 5000
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    
    for i in range(LIMIT):
        ts = int(datetime.datetime.now().strftime('%s'))
        for entry in input_data:
            entry["TimeStamp"] = ts + random.randint(1000, 10000000)
            entry["price"] = random.randint(50, 900)
            output = json.dumps(entry).encode('utf-8')
            producer.send("stock_prices", output)
        if i % 2000 == 0:
            print(i)

if __name__ == "__main__":
   run()