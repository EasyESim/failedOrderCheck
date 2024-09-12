import os
import json
import urllib3
from botocore.exceptions import ParamValidationError
import boto3

class EmailClient:
    def __init__(self):
        self.api_key = os.environ['SEND_GRID_API_KEY']
        self.s3_client = boto3.client('s3')
        self.s3_bucket_name = 'esim-qrcode'
    
    def send_email_with_qr_code(self, email_to, qr_code_binary, esim_details, order_no):
        try:
            # Create an instance of urllib3 PoolManager
            http = urllib3.PoolManager()
            
            # Iterate through the list of image data dictionaries
            for index, image_data in enumerate(qr_code_binary):
                image_name = image_data['image_name']
                
                # Generate a subject based on the number of QR codes
                if len(qr_code_binary) > 1:
                    subject = "Your eSIM details ("+ str(index+1)+" of "+ str(len(qr_code_binary))  +"). Order Number " + str(order_no)
                else:
                    subject = "Your eSIM details. Order Number " + str(order_no)

                try:
                    for esim in esim_details:
                        if os.path.splitext(image_name)[0] == esim['iccid']:
                            bundle = esim['bundle']
                            matchingId = esim['matchingId']
                            rspUrl = esim['rspUrl']
                            esim_title = esim['title']
                    qr_code_url = 'https://esim-qrcode.s3.eu-west-2.amazonaws.com/' + image_name

                    with open('qr_code_email_template.html', 'r') as template_file:
                        email_template = template_file.read()

                    parts = bundle.split('_')
                    
                    # Check if the bundle contains 'esim_UL_'
                    if 'esim_UL_' in bundle:
                        days = parts[2][:-1]  # This strips the last character assuming it's always 'D' for 'Day'
                        if days.isdigit():  # Ensure that the extracted part is a digit
                            formatted_bundle = f"Unlimited - {days} Day{'s' if int(days) > 1 else ''}"
                        else:
                            formatted_bundle = "Unlimited"
                    else:
                        formatted_bundle = parts[1] if len(parts) > 1 else bundle

                    # Format the template with the dynamic URL
                    email_template = email_template.replace('{{esim_title}}', esim_title)
                    email_template = email_template.replace('{{bundle}}', formatted_bundle)
                    email_template = email_template.replace('{{qr_code_url}}', qr_code_url)
                    email_template = email_template.replace('{{matchingId}}', matchingId)
                    email_template = email_template.replace('{{rspUrl}}', rspUrl)

                    # Create the email data for SendGrid API
                    email_data = {
                        "personalizations": [
                            {
                                "to": [{"email": email_to}],
                                "bcc": [{"email": "esimdetails@easyesim.co"}], 
                                "subject": subject
                            }
                        ],
                        "from": {"email": "hello@easyesim.co", "name": 'Easy eSIM'},
                        "content": [
                            {
                                "type": "text/html",
                                "value": email_template
                            }
                        ]
                    }

                    # Serialize email_data to JSON
                    encoded_data = json.dumps(email_data).encode('utf-8')

                    # Send the email using the SendGrid API via urllib3
                    headers = {
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    }
                    response = http.request(
                        'POST',
                        'https://api.sendgrid.com/v3/mail/send',
                        body=encoded_data,
                        headers=headers
                    )

                    if response.status == 202:
                        print(f"Email sent successfully with status code: {response.status}")
                        return True
                    else:
                        print(f"Email sending failed with status code: {response.status}")
                        return False

                except ParamValidationError as e:
                    print(f"Skipping invalid image data: {str(e)}")
                    return False

        except Exception as e:
            print(f"Error sending email: {str(e)}")
            return False
