#
# Copyright © 2023 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
from email.message import EmailMessage
import logging
import os

import boto3
from boto3.dynamodb.conditions import Key
from gcn_kafka import Consumer, config_from_env
from ratelimit import limits, RateLimitException
import backoff

from .helpers import periodic_task
from . import metrics

SESV2 = boto3.client("sesv2")
SENDER = f'GCN Notices <{os.environ["EMAIL_SENDER"]}>'

# Maximum send rate
MAX_SENDS = boto3.client("ses").get_send_quota()["MaxSendRate"]

log = logging.getLogger(__name__)


def get_email_notification_subscription_table():
    client = boto3.client("ssm")
    result = client.get_parameter(
        Name="/RemixGcnProduction/tables/email_notification_subscription"
    )
    table_name = result["Parameter"]["Value"]
    return boto3.resource("dynamodb").Table(table_name)


def query_and_project_subscribers(table, topic):
    """
    Query for subscribed emails for a given topic
    :param topic: The topic for a consumed kafka notification.
    :return: The list of recipient emails.
    """
    try:
        response = table.query(
            IndexName="byTopic",
            ProjectionExpression="#topic, recipient",
            ExpressionAttributeNames={"#topic": "topic"},
            KeyConditionExpression=(Key("topic").eq(topic)),
        )
    except Exception:
        log.exception("Failed to query recipients")
        return []
    else:
        return [x["recipient"] for x in response["Items"]]


def connect_as_consumer():
    log.info("Connecting to Kafka")
    return Consumer(config_from_env(), **{"enable.auto.commit": False})


@periodic_task(86400)
def subscribe_to_topics(consumer: Consumer):
    # list_topics also contains some non-topic values, filtering is necessary
    # This may need to be updated if new topics have a format different than
    # 'gcn.classic.[text | voevent | binary].[topic]'
    topics = [
        topic for topic in consumer.list_topics().topics if topic.startswith("gcn.")
    ]
    log.info("Subscribing to topics: %r", topics)
    consumer.subscribe(topics)


def kafka_message_to_email(message):
    topic = message.topic()
    email_message = EmailMessage()
    if topic.startswith("gcn.classic.text."):
        email_message.set_content(message.value().decode())
    elif topic.startswith("gcn.classic.voevent."):
        email_message.add_attachment(
            message.value(),
            filename="notice.xml",
            maintype="application",
            subtype="xml",
        )
    else:
        email_message.add_attachment(
            message.value(),
            filename="notice.bin",
            maintype="application",
            subtype="octet-stream",
        )
    email_message["Subject"] = topic
    return email_message.as_bytes()


def recieve_alerts(consumer):
    table = get_email_notification_subscription_table()
    while True:
        for message in consumer.consume(timeout=1):
            error = message.error()
            if error is not None:
                log.error("Error consuming message: %s", error)
                continue
            topic = message.topic()
            metrics.received.labels(topic).inc()
            recipients = query_and_project_subscribers(table, topic)
            if recipients:
                log.info("Sending message for topic %s", topic)
                email = kafka_message_to_email(message)
                for recipient in recipients:
                    try:
                        send_raw_ses_message_to_recipient(email, recipient)
                    except Exception:
                        log.exception("Failed to send message")
                    else:
                        metrics.sent.labels(topic, recipient).inc()
            consumer.commit(message)


# Alternatively, we can import sleep_and_retry from ratelimit
# This will cause the thread to sleep until the time limit has ellapsed and
# then retry the call
@backoff.on_exception(
    backoff.expo,
    (
        RateLimitException,
        SESV2.exceptions.LimitExceededException,
        SESV2.exceptions.SendingPausedException,
        SESV2.exceptions.TooManyRequestsException,
    ),
    max_time=300,
)
@limits(calls=MAX_SENDS, period=1)
@metrics.send_request_latency_seconds.time()
def send_raw_ses_message_to_recipient(bytes, recipient):
    SESV2.send_email(
        FromEmailAddress=SENDER,
        Destination={"ToAddresses": [recipient]},
        Content={"Raw": {"Data": bytes}},
    )
