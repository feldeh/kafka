from fastapi import FastAPI, Request
from time import sleep
import json
from kafka import KafkaProducer

app = FastAPI()


@app.post("/data")
async def data(data: dict):

    print(data)
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    producer.send('delhaize_shop',
                  value=data)

    producer.flush()
    sleep(0.1)

    return {"status": "ok"}
