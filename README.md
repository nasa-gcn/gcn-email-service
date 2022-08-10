# gcn-email-service
A hosted listener service to consume alerts from the GCN Kafka broker and send emails to users who subscribe through the new GCN site 

This project requires an IAM role to provide permissions to AmazonDynamoDBReadonlyAccess and AmazonSESFullAccess which will allow for reading from the subscribers table and sending SES email notification respectively.  