import uuid
from lineitem import LineItem


class Order:
    def __init__(self, payload):

        self.id = "O-" + str(uuid.uuid4())
        self.source_name = 'shopify'
        self.source_order_id = payload['id']
        self.source_order_number = payload['order_number']
        self.price = payload['total_price']
        self.order_items = []

        raw_items = payload['line_items']
        for item in raw_items:
            print(item)
            self.order_items.append(
                LineItem(item['sku'], item['price'], item['quantity']).asdict())
