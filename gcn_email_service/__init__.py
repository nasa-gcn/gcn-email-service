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

# TODO: update the sender for test and prod
SENDER = "GCN Alerts <alerts@dev.gcn.nasa.gov>"
CHARSET = "UTF-8"
SUBJECT = "GCN/{}"
# Used for testing attachment sends, works for a local file
# Can probably be used to draw from a bucket, also still need to try
# for multiple files
ATTACHMENT = ""

# Maximum send rate = 14 emails / Second
MAX_SENDS = 14
SENDING_PERIOD = 1  # Seconds

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
    except ClientError:
        logger.exception('Failed to query recipients')
        return []
    else:
        return [x['recipient'] for x in response['Items']]


def connect_as_consumer():
    global consumer
    consumer = Consumer(config_from_env())


def subscribe_to_topics():
    # list_topics also contains some non-topic values, filtering is necessary
    # This may need to be updated if new topics have a format different than
    # 'gcn.classic.[text | voevent | binary].[topic]'
    topics = [
        topic for topic in consumer.list_topics().topics
        if topic.startswith('gcn.')]
    consumer.subscribe(topics)


def recieve_alerts():
    table = boto3.resource(
        'dynamodb'
    ).Table(
        'table name here'
    )
    ses = boto3.client('ses')
    while True:
        for message in consumer.consume():
            recipients = query_and_project_subscribers(
                table, message.topic()
            )
            if recipients:
                for recipient in recipients:
                    send_raw_ses_message_to_recipient(ses, message, recipient)
                    # send_ses_message_to_recipient(ses, message, recipient)


# Alternatively, we can import sleep_and_retry from ratelimit
# This will cause the thread to sleep until the time limit has ellapsed and
# then retry the call
@on_exception(expo, RateLimitException)
@limits(calls=MAX_SENDS, period=SENDING_PERIOD)
def send_raw_ses_message_to_recipient(client, message, recipient):
    BODY_TEXT = str(email.message_from_bytes(message.value()))

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
        att = MIMEApplication(open(ATTACHMENT, 'rb').read())

        # Add a header to tell the email client to treat this part as an
        # attachment, and to give the attachment a name.
        att.add_header(
            'Content-Disposition', 'attachment',
            filename=os.path.basename(ATTACHMENT))
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
@limits(calls=MAX_SENDS, period=SENDING_PERIOD)
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
    connect_as_consumer()
    subscribe_to_topics()
    recieve_alerts()
