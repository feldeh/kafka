from kafka import KafkaConsumer
from json import loads
from time import sleep


consumer = KafkaConsumer(
    'delhaize_shop',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)
consumer.subscribe(topics=["delhaize_shop"])


for event in consumer:

    print("value: ", event.value)

    sleep(0.5)







