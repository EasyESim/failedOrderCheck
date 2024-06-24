import uuid
from datetime import datetime, timezone

class Customer:
    def __init__(self, customer_id, source_name, source_customer_id, orders):
        self.customer_id = customer_id
        self.source_name = source_name
        self.source_customer_id = source_customer_id
        self.orders = orders
        self.upserted_at = str(datetime.now(timezone.utc).isoformat())

    @classmethod
    def from_dynamo(cls, dict):
        cust = Customer(dict['customer_id'], dict['source_name'], dict['source_customer_id'], dict['orders'])
        return cust
    
    @classmethod
    def from_payload(cls, payload, order):
        cust = Customer("C-"+str(uuid.uuid4()), "shopify", payload['customer']['id'], [order.id])
        return cust
    
    def addOrder(self, order):
        self.orders.append(order.id)