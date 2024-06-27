from datetime import datetime
import logging
from dynamo_client import DynamoClient
from esim_go_client import EsimGoClient
from send_email import EmailClient

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    dynamo_client = DynamoClient()
    email_client = EmailClient()
    esim_client = EsimGoClient()

    # Specify the start date
    start_date = datetime(2024, 6, 22).strftime('%Y-%m-%d')
    
    # Specify the list of statuses to scan for
    failed_statuses = [
        'esim_order_creation_failed', 
        'dynamodb_esim_order_creation_failed', 
        'esim_details_retrieval_failed', 
        'esim_qrcode_retrieval_failed', 
        'dynamodb_qrcode_retrieval_failed', 
        'dynamodb_esim_details_retrieval_failed'
    ]
    
    # Scan for orders with specific failed statuses and from the specified start date
    response = dynamo_client.scan_orders_with_failed_statuses(start_date, failed_statuses)

    for order in response:
        order_id = order['order_id']
        current_status = order['order_status']
        
        logger.info("Processing order: %s with status: %s", order_id, current_status)

        if current_status == 'esim_order_creation_failed':
            esim_order_details = esim_client.new_order(order)
            if not esim_order_details:
                logger.error("Failed to generate a new order in EsimGo: %s", str(order))
                dynamo_client.update_order_status(order_id, "esim_order_creation_failed")
                continue
            else:
                dynamo_client.update_order_status(order_id, "esim_order_created")

        if current_status in ['esim_order_creation_failed', 'esim_order_created', 'dynamodb_esim_order_creation_failed']:
            order_id = dynamo_client.put_esim_order(esim_order_details, order)
            if not order_id:
                logger.error("Failed to generate a new order: %s", str(esim_order_details))
                dynamo_client.update_order_status(order_id, "dynamodb_esim_order_creation_failed")
                continue
            else:
                dynamo_client.update_order_status(order_id, "esim_order_saved")

        if current_status in ['esim_order_creation_failed', 'esim_order_created', 'dynamodb_esim_order_creation_failed', 'esim_order_saved']:
            esim_order_details = esim_client.get_esim_details(order_id)
            if not esim_order_details:
                logger.error("Failed to get eSIM details: %s", str(esim_order_details))
                dynamo_client.update_order_status(order_id, "esim_details_retrieval_failed")
                continue
            else:
                dynamo_client.update_esim_order(order_id, esim_order_details, order['line_items'])

        if current_status in ['esim_order_creation_failed', 'esim_order_created', 'dynamodb_esim_order_creation_failed', 'esim_order_saved', 'esim_details_retrieval_failed']:
            image_data = esim_client.get_esim_qrcode(order_id)
            if not image_data:
                logger.error("Failed to get eSIM QR code details: %s", str(esim_order_details))
                dynamo_client.update_order_status(order_id, "esim_qrcode_retrieval_failed")
                continue
            else:
                dynamo_client.update_esim_qr_code(order_id, image_data)

        if current_status in ['esim_order_creation_failed', 'esim_order_created', 'dynamodb_esim_order_creation_failed', 'esim_order_saved', 'esim_details_retrieval_failed', 'esim_qrcode_retrieval_failed']:
            qr_codes = dynamo_client.get_qr_code_from_db(order_id)
            if not qr_codes:
                logger.error("Failed to get QR codes from the database: %s", str(qr_codes))
                dynamo_client.update_order_status(order_id, "dynamodb_qrcode_retrieval_failed")
                continue

            esim_details = dynamo_client.get_esim_from_db(order_id)
            if not esim_details:
                logger.error("Failed to get eSIM details from the database: %s", str(esim_details))
                dynamo_client.update_order_status(order_id, "dynamodb_esim_details_retrieval_failed")
                continue

            email_client.send_email_with_qr_code(order['contact_email'], qr_codes, esim_details, order['order_number'])
            dynamo_client.update_order_status(order_id, "email_sent")
