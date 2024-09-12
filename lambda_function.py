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

    start_date = datetime(2024, 6, 22).strftime('%Y-%m-%d')
    failed_statuses = [
        'esim_order_creation_failed', 
        'dynamodb_esim_order_creation_failed', 
        'esim_details_retrieval_failed', 
        'esim_qrcode_retrieval_failed', 
        'dynamodb_qrcode_retrieval_failed', 
        'dynamodb_esim_details_retrieval_failed',
        'qrcode_data_not_found'
    ]
    
    response = dynamo_client.scan_orders_with_failed_statuses(start_date, failed_statuses)

    for order in response:
        order_id = order['order_id']
        logger.info("Order Id: %s", order_id)
        current_status = order['order_status']
        logger.info("current_status: %s", current_status)
        esim_order_details_from_db = dynamo_client.get_esim_details_from_db_using_order_ref_id(order_id)
        logger.info("esim_order_details_from_db: %s", esim_order_details_from_db)
        logger.info("Processing order: %s with status: %s", order_id, current_status)

        # Initialize qr_codes to avoid the 'referenced before assignment' error
        qr_codes = None

        try:
            if current_status == 'esim_order_creation_failed':
                logger.info("============================")
                logger.info("Processing Step 1")
                esim_order_details = esim_client.new_order(order)
                if not esim_order_details:
                    raise Exception("Failed to generate a new order in EsimGo")
                else:
                    logger.info("esim_order_created")
                    dynamo_client.update_order_status(order_id, "esim_order_created")
                    logger.info("Process Done")
                    logger.info("============================")

            if current_status in ['esim_order_creation_failed', 'dynamodb_esim_order_creation_failed']:
                logger.info("============================")
                logger.info("Processing Step 2")
                esim_order_details = esim_client.new_order(order)
                order_id = dynamo_client.put_esim_order(esim_order_details, order)
                if not order_id:
                    raise Exception("Failed to generate a new order in DynamoDB")
                else:
                    logger.info("esim_order_saved")
                    dynamo_client.update_order_status(order_id, "esim_order_saved")
                    logger.info("Process Done")
                    logger.info("============================")

            if current_status in ['esim_order_creation_failed', 'dynamodb_esim_order_creation_failed', 'esim_details_retrieval_failed', 'dynamodb_esim_details_retrieval_failed']:
                logger.info("============================")
                logger.info("Processing Step 3")
                esim_order_details = esim_client.get_esim_details(esim_order_details_from_db['esim_order_id'])
                if not esim_order_details:
                    raise Exception("Failed to get eSIM details from EsimGo")
                else:
                    logger.info("esim_details_retrieved")
                    dynamo_client.update_order_status(order_id, "esim_details_retrieved")
                    logger.info("Process Done")
                    logger.info("============================")
            
            if current_status in ['esim_order_creation_failed', 'dynamodb_esim_order_creation_failed', 'esim_details_retrieval_failed', 'dynamodb_esim_details_retrieval_failed', 'esim_qrcode_retrieval_failed', 'dynamodb_qrcode_retrieval_failed', 'qrcode_data_not_found']:
                logger.info("============================")
                logger.info("Processing Step 4")
                qr_codes = esim_client.get_esim_qrcode(esim_order_details_from_db['esim_order_id'])
                if not qr_codes:
                    raise Exception("Failed to retrieve QR code from EsimGo")
                else:
                    logger.info("esim_qrcode_retrieved")
                    dynamo_client.update_order_status(order_id, "esim_qrcode_retrieved")
                    logger.info("Process Done")
                    logger.info("============================")

            if current_status in ['esim_order_creation_failed', 'dynamodb_esim_order_creation_failed', 'esim_details_retrieval_failed', 'dynamodb_esim_details_retrieval_failed', 'esim_qrcode_retrieval_failed', 'dynamodb_qrcode_retrieval_failed', 'qrcode_data_not_found']:
                logger.info("============================")
                logger.info("Processing Step 5")
                result = dynamo_client.update_esim_qr_code(esim_order_details_from_db['esim_order_id'], qr_codes)
                if not result:
                    raise Exception("Failed to update QR code in DynamoDB")
                else:
                    logger.info("dynamodb_qrcode_retrieved")
                    dynamo_client.update_order_status(order_id, "dynamodb_qrcode_retrieved")
                    logger.info("Process Done")
                    logger.info("============================")

            if current_status in ['esim_order_creation_failed', 'dynamodb_esim_order_creation_failed', 'esim_details_retrieval_failed', 'dynamodb_esim_details_retrieval_failed', 'esim_qrcode_retrieval_failed', 'dynamodb_qrcode_retrieval_failed', 'qrcode_data_not_found']:
                logger.info("============================")
                logger.info("Processing Step 6")
                result = email_client.send_email_with_qr_code(esim_order_details_from_db['email_id'], qr_codes, esim_order_details_from_db['esim_details'], esim_order_details_from_db['shopify_order_id'])
                if not result:
                    raise Exception("Failed to send email with QR codes")
                else:
                    logger.info("email_sent")
                    esim_client.update_esim(esim_order_details_from_db['esim_details'], esim_order_details_from_db['shopify_order_id'])
                    dynamo_client.update_order_status(order_id, "email_sent")
                    logger.info("Process Done")
                    logger.info("============================")

        except Exception as e:
            logger.error("Error processing order %s: %s", order_id, str(e))
            if current_status not in ['email_sent']:
                dynamo_client.update_order_status(order_id, current_status)

    return {
        'statusCode': 200,
        'body': 'Processing completed'
    }
