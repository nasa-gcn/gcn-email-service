#
# Copyright © 2022 United States Government as represented by the Administrator
# of the National Aeronautics and Space Administration. No copyright is claimed
# in the United States under Title 17, U.S. Code. All Other Rights Reserved.
#
# SPDX-License-Identifier: NASA-1.3
#
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import logging
import os

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from gcn_kafka import Consumer
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo

# TODO: update the sender for test and prod
SENDER = "GCN Alerts <alerts@dev.gcn.nasa.gov>"
AWS_REGION = "us-east-1"
CHARSET = "UTF-8"
SUBJECT = "GCN/{}"
# Used for testing attachment sends, works for a local file
# Can probably be used to draw from a bucket, also still need to try
# for multiple files
ATTACHMENT=""

# Maximum send rate = 14 emails / Second
MAX_SENDS = 14
SENDING_PERIOD = 1 # Seconds

logger = logging.getLogger(__name__)


def query_and_project_subscribers(table, topic):
    """
    Query for subscribed emails for a given topic
    :param topic: The topic for a consumed kafka notification.
    :return: The list of recipient emails.
    """
    try:
        response = table.query(
            IndexName='byTopic',
            ProjectionExpression="#topic, recipient",
            ExpressionAttributeNames={"#topic": "topic"},
            KeyConditionExpression=(Key('topic').eq(topic)))
    except ClientError as err:
        if err.response['Error']['Code'] == "ValidationException":
            logger.warning(
                "There's a validation error. Here's the message: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
        else:
            logger.error(
                "Couldn't query for recipients. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    else:
        return [x['recipient'] for x in response['Items']]


def connect_as_consumer():
    global consumer
    # TODO: dedicated credentials for service?
    consumer = Consumer(client_id='fill-me-in',
                        client_secret='fill-me-in')


def subscribe_to_topics():
    # list_topics also contains some non-topic values, filtering is necessary
    # This may need to be updated if new topics have a format different than 'gcn.classic.[text | voevent | binary].[topic]'
    topics = list(topic for topic in consumer.list_topics().topics if 'gcn' in topic)
    consumer.subscribe(topics)


def recieve_alerts():
    try:
        while True:
            for message in consumer.consume():
                table = boto3.resource('dynamodb', region_name=AWS_REGION).Table('table name here')
                recipients = query_and_project_subscribers(table, message.topic())
                if recipients:
                    for recipient in recipients:
                        send_raw_ses_message_to_recipient(message, recipient)
                        #send_ses_message_to_recipient(message, recipient)

    except KeyboardInterrupt:
        print('Interrupted')


# Alternatively, we can import sleep_and_retry from ratelimit
# This will cause the thread to sleep until the time limit has ellapsed and then retry the call
@on_exception(expo, RateLimitException)
@limits(calls=MAX_SENDS, period=SENDING_PERIOD)
def send_raw_ses_message_to_recipient(message, recipient):
    BODY_TEXT = str(email.message_from_bytes(message.value()))

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION)

    # multipart/mixed parent container
    msg = MIMEMultipart('mixed')
    print(msg.get_content_type())
    msg['Subject'] = SUBJECT.format(message.topic().split('.')[3])
    msg['From'] = SENDER
    msg['To'] = recipient

    msg_body = MIMEMultipart('alternative')
    text_part = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
    print(text_part)
    msg_body.attach(text_part)

    msg.attach(msg_body)
    # Define attachment part:
    if ATTACHMENT:
    # Define the attachment part and encode it using MIMEApplication.
        att = MIMEApplication(open(ATTACHMENT, 'rb').read())

        # Add a header to tell the email client to treat this part as an attachment,
        # and to give the attachment a name.
        att.add_header('Content-Disposition', 'attachment', filename=os.path.basename(ATTACHMENT))
        msg.attach(att)

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_raw_email(
           Source=SENDER,
           Destinations=[recipient],
           RawMessage={
            'Data':msg.as_string()
           }
        )

    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


@on_exception(expo, RateLimitException)
@limits(calls=MAX_SENDS, period=SENDING_PERIOD)
def send_ses_message_to_recipient(message, recipient):
    BODY_TEXT = str(email.message_from_bytes(message.value()))
    # Might not need this
    BODY_HTML = str(email.message_from_bytes(message.value()))

    # TODO: Include unsub link or link to notification management in gcn?

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION)
    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    recipient,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT.format(message.topic().split('.')[3]),
                },
            },
            Source=SENDER
        )

    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def main():
    connect_as_consumer()
    subscribe_to_topics()
    recieve_alerts()
