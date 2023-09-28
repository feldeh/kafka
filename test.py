data = {
    "id": "123456789",
    "store": "Brussels",
    "date": "2020-01-01",
    "products": [
        {
            "id": "123456789",
            "name": "Banana",
            "price": 1.5
        },
        {
            "id": "123456789",
            "name": "Bread",
            "price": 1.5
        },
        {
            "id": "123456789",
            "name": "Water",
            "price": 1.5
        }
    ]
}


def add_total(data):
    total_price = 0
    for product in data['products']:
        total_price += product['price']
    data['total_price'] = total_price
    return data


# total_price = 0

# for product in data['products']:
#     total_price += product['price']


# data['total_price'] = total_price


category = {
    "Fruit": ["apple", "banana", "orange", "pear", "kiwi"],
    "Bakery": ["bread", "croissant", "baguette", "cake"],
    "Drink": ["water", "soda", "beer", "wine"]
}


def process_data(data):
    total_price = 0
    for product in data['products']:
        for k in category:
            if product['name'].lower() in category[k]:
                product['category'] = k
        total_price += product['price']
    data['total_price'] = total_price
    return data


print(process_data(data))
