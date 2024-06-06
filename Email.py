import requests
import json
import base64
import logging
from datetime import datetime
from log import update_email
logging.basicConfig(filename='logfile.log', level=logging.INFO)

dt_hr = datetime.now()
dt_hr = dt_hr.strftime('%d/%m/%Y %H:%M')

# Função para enviar e-mail
def send_email(access_token, subject, body, to_email, attachment_path, name_file, superior):
    graph_api_url = 'https://graph.microsoft.com/v1.0/me/sendMail'

    with open(attachment_path, 'rb') as attachment_file:
        attachment_content = attachment_file.read() 
    attachment_content_base64 = base64.b64encode(attachment_content).decode('utf-8') 
    
    email_message = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": to_email
                    }
                }
            ],
            "ccRecipients": [
            {
                "emailAddress": {
                "address": ""  
                }
            },
            ],
            "attachments":[
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": name_file,
                    "contentType": "application/xml",
                    "contentBytes": attachment_content_base64
                }
            ]       
        }
    }

    email_json = json.dumps(email_message)

    headers = {
        "Authorization": "Bearer " + access_token,
        "Content-Type": "application/json"
    }

    response = requests.post(graph_api_url, headers=headers, data=email_json)

    if response.status_code == 202:
            update_email(
                 att="SIM",
                 path=attachment_path
            )
            pass
    else:
        conection_error = (f"{dt_hr} - Arquivo: Email.py - Erro de conexão: {response.status_code} {response.text}")
        print(f"{conection_error}")
        logging.error(conection_error)

def get_acess_token():
    client_id = ""
    client_secret = ""
    scopes = ["https://graph.microsoft.com/.default"]
    token_url = "https://login.microsoftonline.com/f11c815a-6b17-4f64-acbd-f5c8662147f4/oauth2/v2.0/token"

    token_data = {
        'grant_type': 'password',
        'scope': ' '.join(scopes),
        'client_id': client_id,
        'client_secret': client_secret,
        'username': '',
        'password': ''
    }

    token_response = requests.post(token_url, data=token_data)
    
    # Verica se a solicitação de token foi bem-sucedida
    if token_response.status_code == 200:
        token_json = token_response.json()
        access_token = token_json.get('access_token')
        return access_token
    
    else:
        token_erro = (f"{dt_hr} - Arquivo: Email.py - Erro no Token: {token_response.status_code} {token_response.text}")
        logging.error(token_erro)
        print(f"{token_erro}")
        return None

