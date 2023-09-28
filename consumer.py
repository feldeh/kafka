from kafka import KafkaConsumer
from json import loads
from time import sleep


PRODUCT_CATEGORIES = {
    "Fruit": ["apple", "banana", "orange", "pear", "kiwi"],
    "Bakery": ["bread", "croissant", "baguette", "cake"],
    "Drink": ["water", "soda", "beer", "wine"]
}

def process_data(data):
    total_price = 0
    for product in data['products']:
        for cat in PRODUCT_CATEGORIES:
            if product['name'].lower() in PRODUCT_CATEGORIES[cat]:
                product['category'] = cat
        total_price += product['price']
    data['total_price'] = total_price
    return data


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

    event = event.value
    processed_event = process_data(event)
    sleep(0.5)







