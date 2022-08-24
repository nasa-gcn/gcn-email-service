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
from gcn_kafka import Consumer, config_from_env
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo

from .helpers import periodic_task

SENDER = f'GCN Alerts <{os.environ["EMAIL_SENDER"]}>'

CHARSET = "UTF-8"
SUBJECT = "GCN/{}"
# Used for testing attachment sends, works for a local file
# Can probably be used to draw from a bucket, also still need to try
# for multiple files

SES = boto3.client('ses')
# Maximum send rate
MAX_SENDS = SES.get_send_quota()['MaxSendRate']

logger = logging.getLogger(__name__)


def get_email_notification_subscription_table():
    client = boto3.client('ssm')
    result = client.get_parameter(
        Name='/RemixGcnProduction/tables/email_notification_subscription')
    table_name = result['Parameter']['Value']
    return boto3.resource('dynamodb').Table(table_name)


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
    except ClientError:
        logger.exception('Failed to query recipients')
        return []
    else:
        return [x['recipient'] for x in response['Items']]


def connect_as_consumer():
    return Consumer(config_from_env())


@periodic_task(86400)
def subscribe_to_topics(consumer):
    # list_topics also contains some non-topic values, filtering is necessary
    # This may need to be updated if new topics have a format different than
    # 'gcn.classic.[text | voevent | binary].[topic]'
    topics = [
        topic for topic in consumer.list_topics().topics
        if topic.startswith('gcn.')]
    consumer.subscribe(topics)


def recieve_alerts(consumer):
    table = get_email_notification_subscription_table()

    while True:
        for message in consumer.consume():
            recipients = query_and_project_subscribers(
                table, message.topic()
            )
            for recipient in recipients:
                send_raw_ses_message_to_recipient(SES, message, recipient)
                # send_ses_message_to_recipient(ses, message, recipient)


# Alternatively, we can import sleep_and_retry from ratelimit
# This will cause the thread to sleep until the time limit has ellapsed and
# then retry the call
@on_exception(expo, RateLimitException)
@limits(calls=MAX_SENDS, period=1)
def send_raw_ses_message_to_recipient(client, message, recipient):
    BODY_TEXT = ""
    ATTACHMENT = False
    if '.text.' in message.topic():
        BODY_TEXT = str(email.message_from_bytes(message.value()))

    else:
        ATTACHMENT = True

    # multipart/mixed parent container
    msg = MIMEMultipart('mixed')
    msg['Subject'] = SUBJECT.format(message.topic().split('.')[3])
    msg['From'] = SENDER
    msg['To'] = recipient

    msg_body = MIMEMultipart('alternative')
    text_part = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
    msg_body.attach(text_part)

    msg.attach(msg_body)
    # Define attachment part:
    if ATTACHMENT:
        # Define the attachment part and encode it using MIMEApplication.
        att = MIMEApplication(message.value())

        # Add a header to tell the email client to treat this part as an
        # attachment, and to give the attachment a name.
        fileExt = ".xml" if '.voevent.' in message.topic() else ".bin"
        att.add_header(
            'Content-Disposition', 'attachment',
            filename=os.path.basename(f"notification{fileExt}"))
        msg.attach(att)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        client.send_raw_email(
           Source=SENDER,
           Destinations=[recipient],
           RawMessage={
            'Data': msg.as_string()
           }
        )

    # Display an error if something goes wrong.
    except ClientError:
        logger.exception('Failed to send message')


@on_exception(expo, RateLimitException)
@limits(calls=MAX_SENDS, period=1)
def send_ses_message_to_recipient(client, message, recipient):
    BODY_TEXT = str(email.message_from_bytes(message.value()))
    # Might not need this
    BODY_HTML = str(email.message_from_bytes(message.value()))

    # TODO: Include unsub link or link to notification management in gcn?

    # Try to send the email.
    try:
        # Provide the contents of the email.
        client.send_email(
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
    except ClientError:
        logger.exception('Failed to send message')


def main():
    consumer = connect_as_consumer()
    subscribe_to_topics(consumer)
    recieve_alerts(consumer)
