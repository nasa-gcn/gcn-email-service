# gcn-email

A hosted listener service to consume alerts from the GCN Kafka broker and send emails to users who subscribe through the new GCN site.

## Configuration

The following environment variables may be used to configure the service:

| Name                 | Value                                                                              |
| -------------------- | ---------------------------------------------------------------------------------- |
| `AWS_DEFAULT_REGION` | AWS region, e.g. `us-east-1`                                                       |
| `KAFKA_*`            | Kafka client configuration as understood by [Confluent Platform docker containers] |
| `EMAIL_SENDER`       | Address to be used as the sender for emails sent through SES                       |

This project requires an IAM role to provide permissions to AmazonSSMReadOnlyAccess, AmazonDynamoDBReadonlyAccess, and AmazonSESFullAccess which will allow for accessing the value of the stored subscribers table parameter, reading from that table, and sending SES email notifications respectively.

[Confluent Platform docker containers]: https://docs.confluent.io/platform/current/installation/docker/config-reference.html
