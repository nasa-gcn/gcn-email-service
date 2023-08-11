#
# Copyright © 2023 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
"""Prometheus metrics."""
import prometheus_client

received = prometheus_client.Counter(
    'received',
    'Kafka messages received',
    labelnames=['topic'],
    namespace=__package__)


sent = prometheus_client.Counter(
    'sent',
    'Emails sent',
    labelnames=['topic', 'address'],
    namespace=__package__)


send_request_latency_seconds = prometheus_client.Histogram(
    'send_request_latency_seconds',
    'Time taken to send each message',
    namespace=__package__)
