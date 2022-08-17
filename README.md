# gcn-email-service

A hosted listener service to consume alerts from the GCN Kafka broker and send emails to users who subscribe through the new GCN site.

## Configuration

The following environment variables may be used to configure the service:

| Name                 | Value                                                                              |
| -------------------- | ---------------------------------------------------------------------------------- |
| `AWS_DEFAULT_REGION` | AWS region, e.g. `us-east-1`                                                       |
| `KAFKA_*`            | Kafka client configuration as understood by [Confluent Platform docker containers] |

This project requires an IAM role to provide permissions to AmazonDynamoDBReadonlyAccess and AmazonSESFullAccess which will allow for reading from the subscribers table and sending SES email notification respectively.

[Confluent Platform docker containers]: https://docs.confluent.io/platform/current/installation/docker/config-reference.html
