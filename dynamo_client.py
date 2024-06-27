import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from datetime import datetime, timezone
import json
import logging

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DynamoClient:
    def __init__(self):
        client = boto3.resource('dynamodb')
        self.order_table = client.Table("order")
        self.cust_table = client.Table("customer")
        self.esim_table = client.Table("esim_details")

    def get_customers(self, source_customer_id):
        response = self.cust_table.query(
            IndexName='source_customer_id-index',
            KeyConditionExpression=Key('source_customer_id').eq(source_customer_id)
        )
        logger.info("Get customers response: %s", response)
        return response['Items']

    def put_customer(self, customer):
        response = self.cust_table.put_item(Item={
            'customer_id': customer.customer_id,
            'source_name': customer.source_name,
            'source_customer_id': customer.source_customer_id,
            'orders': customer.orders,
            'upserted_at': customer.upserted_at
        })
        success = response['ResponseMetadata']['HTTPStatusCode'] == 200
        logger.info("Put customer success: %s", success)
        return success

    def put_order(self, order, customer):
        response = self.order_table.put_item(Item={
            'order_id': order.id,
            'source_name': order.source_name,
            'source_order_id': order.source_order_id,
            'customer_id': customer.customer_id,
            'total_price': Decimal(str(order.price)),
            'order_items': order.order_items,
            'upserted_at': str(datetime.now(timezone.utc).isoformat())
        })
        success = response['ResponseMetadata']['HTTPStatusCode'] == 200
        logger.info("Put order success: %s", success)
        return success

    def put_esim_order(self, esim_order, order_table_ref):
        esim_order = json.loads(esim_order)
        esim_order['total'] = Decimal(str(esim_order['total']))
        for order_item in esim_order['order']:
            order_item['subTotal'] = Decimal(str(order_item['subTotal']))
            order_item['pricePerUnit'] = Decimal(str(order_item['pricePerUnit']))
        response = self.esim_table.put_item(Item={
            'esim_order_id': esim_order['orderReference'],
            'order_table_ref_id': order_table_ref.id,
            'status': esim_order['status'],
            'currency': esim_order['currency'],
            'total': esim_order['total'],
            'order': esim_order['order'],
            'upserted_at': str(datetime.now(timezone.utc).isoformat())
        })
        logger.info("Put eSIM order response: %s", response)
        return esim_order['orderReference']

    def update_esim_order(self, esim_order_id, new_esim_details, line_items):
        new_esim_details = json.loads(new_esim_details)
        sku_to_title = {item['sku']: item['title'] for item in line_items}
        for esim_detail in new_esim_details:
            sku = esim_detail['bundle']
            if sku in sku_to_title:
                esim_detail['title'] = sku_to_title[sku]
        update_expression = "SET esim_details = :esim_details"
        expression_attribute_values = {":esim_details": new_esim_details}
        response = self.esim_table.update_item(
            Key={'esim_order_id': esim_order_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )
        logger.info("Update eSIM order response: %s", response)
        return response

    def update_esim_qr_code(self, esim_order_id, image_data):
        esim_qr_codes = []
        for img in image_data:
            esim_qr_codes.append({
                "image_name": img["image_name"],
                "image_url": img["image_url"]
            })
        update_expression = "SET esim_qr_codes = :esim_qr_codes"
        expression_attribute_values = {":esim_qr_codes": esim_qr_codes}
        response = self.esim_table.update_item(
            Key={'esim_order_id': esim_order_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )
        logger.info("Update eSIM QR code response: %s", response)
        return response

    def get_qr_code_from_db(self, esim_order_id):
        response = self.esim_table.get_item(
            Key={'esim_order_id': esim_order_id},
        )
        item = response.get('Item')
        logger.info("Get QR code from DB response: %s", response)
        if item and 'esim_qr_codes' in item:
            return item['esim_qr_codes']

    def get_esim_from_db(self, esim_order_id):
        response = self.esim_table.get_item(
            Key={'esim_order_id': esim_order_id},
        )
        item = response.get('Item')
        logger.info("Get eSIM from DB response: %s", response)
        if item and 'esim_details' in item:
            return item['esim_details']

    def update_order_status(self, order_id, status):
        update_expression = "SET order_status = :status"
        expression_attribute_values = {":status": status}
        response = self.order_table.update_item(
            Key={'order_id': order_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )
        logger.info("Update order status response: %s", response)
        return response

    def scan_orders_with_failed_statuses(self, start_date, statuses):
        # Convert the list of statuses into a condition for filtering
        filter_expression = Attr('order_status').is_in(statuses) & Attr('upserted_at').gte(start_date)

        response = self.order_table.scan(
            FilterExpression=filter_expression
        )

        logger.info("Scan orders with failed statuses response: %s", response)
        return response['Items']
