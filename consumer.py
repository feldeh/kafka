from kafka import KafkaConsumer
import json
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
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


event_list = []

with open("database.json", "w") as db:
    json.dump(event_list, db)

for event in consumer:
    with open("database.json", "w") as db:
        event_value = event.value
        print(event_value)
        processed_event = process_data(event_value)
        event_list.append(processed_event)
        json.dump(event_list, db, indent=4)
        sleep(0.1)
