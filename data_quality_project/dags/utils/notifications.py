# dags/utils/notifications.py
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

load_dotenv()

def send_slack_notification(message):
    """Send alert to Slack channel"""
    client = WebClient(token=os.getenv('SLACK_TOKEN'))
    try:
        response = client.chat_postMessage(
            channel="#data-quality-alerts",
            text=message
        )
        print(f"Slack notification sent: {message}")
        return response
    except SlackApiError as e:
        print(f"Error sending slack message: {e.response['error']}")
        return None

def send_email_alert(subject, message):
    """Send email alert to data team"""
    sender = os.getenv('EMAIL_USERNAME')
    recipients = ["data_team@example.com"]
    
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    
    msg.attach(MIMEText(message, 'plain'))
    
    try:
        server = smtplib.SMTP(os.getenv('EMAIL_SMTP_SERVER'), os.getenv('EMAIL_SMTP_PORT'))
        server.starttls()
        server.login(os.getenv('EMAIL_USERNAME'), os.getenv('EMAIL_PASSWORD'))
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        print(f"Email alert sent: {subject}")
        return True
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return False