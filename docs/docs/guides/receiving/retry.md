---
layout: default
title: Handling message failures
nav_order: 4
parent: Receiving messages
grand_parent: How to Guides
---

# Handling message failures
This page explains how to handle message failures by setting a subscription retry policy or forwarding undelivered messages to a dead-letter topic (also know as a dead-letter queue).

## Subscription retry policy
If the Metro attempts to deliver a message but the subscriber can't acknowledge it, Metro will retry sending the message. By default, Metro will try resending the message immediately. However, the conditions that prevented message acknowledgement may not have had time to change when using immediate redelivery, resulting in the message being unacknowledged again, and the message being continuously redelivered. To address this issue, Metro lets you configure an exponential backoff policy for better flow control.

The idea behind exponential backoff is to add progressively longer delays between retry attempts. After the first delivery failure, Metro will wait for a minimum backoff time before retrying. For each consecutive failure on that message, more time will be added to the delay, up to a maximum delay. The maximum and minimum delay intervals are not fixed, and should be configured based on local factors to your application.

## Forwarding to dead-letter topics

If the Metro service attempts to deliver a message but the subscriber can't acknowledge it, Metro can forward the undeliverable message to a dead-letter topic. You can set the maximum number of delivery attempts.

To forward undeliverable messages, complete the following steps:

* Create a topic.
* Create or update a subscription and set the dead-letter topic.
* Permit Metro to forward undeliverable messages to the dead-letter topic and remove forwarded undeliverable messages from the subscription.

A dead-letter topic is a subscription property, not a topic property. When you create a topic, you can't specify that the topic is a dead-letter topic. You create or update a subscription and set the dead-letter topic.

### Setting a dead-letter topic

When you create or update a subscription, you can set a subscription property for the dead-letter topic. For best results, set the dead-letter topic to a different topic from the topic that the subscription is attached to.

If you set a dead-letter topic, you can also set the following subscription properties:

* Maximum number of delivery attempts: The default is 5 delivery attempts and you can specify between 5-100 delivery attempts.
* Project with the dead-letter topic: If the dead-letter topic is in a different project from the subscription, you must specify the project with the dead-letter topic.

You can't enable message ordering and use a dead-letter topic. Metro forwards messages to dead-letter topics on a best-effort basis, which might prevent Metro from redelivering messages in order.

#### Sample Code
Below is a sample subscription with retry and dead letter policy
```javascript
{
    "topic": "projects/project001/topics/topic001",
    "pushConfig": {
        "pushEndpoint": "https://webhook.site/ab19242a-7797-4560-9069-76bcbee23e6"
    },
    "ackDeadlineSeconds": 3,
    "deadLetterPolicy":{
        "maxDeliveryAttempts": 5
    },
    "retryPolicy": {
        "minimumBackoff": "5s",
        "maximumBackoff": "30s"
    }
}
```
In case a `deadLetterTopic` is not provided in the payload, messages would be delivered to a default dead letter topic created by metro whose name would be `<topic-name>-dlq`.

The default for `maxDeliveryAttempts` is 5. It's value can range from 1 to 100.
The default for `minimumBackoff` is 10s. It's value can range from 0s to 600s.
The default for `maximumBackoff` is 600s. It's value can range from 0s to 3600s.