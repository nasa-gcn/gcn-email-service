import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from gcn_kafka import Consumer
import logging
import email

# TODO: update the sender for test and prod 
SENDER = "GCN Alerts <alerts@dev.gcn.nasa.gov>"
AWS_REGION = "us-east-1"
CHARSET = "UTF-8"
SUBJECT = "GCN Alert for Topic: {}"

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
                table = boto3.resource('dynamodb',region_name=AWS_REGION).Table('table name here')
                recipients = query_and_project_subscribers(table, message.topic())

                for recipient in recipients:
                    send_ses_message_to_recipient(message, recipient)
    
    except KeyboardInterrupt:
        print('Interrupted')

def send_ses_message_to_recipient(message, recipient):
    BODY_TEXT = str(email.message_from_bytes(message.value()))
    # Might not need this
    BODY_HTML = str(email.message_from_bytes(message.value()))

    # TODO: Include unsub link?

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION)
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
                    'Data': SUBJECT.format(message.topic()),
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

if (__name__ == "__main__"):
    main()