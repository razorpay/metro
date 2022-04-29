---
layout: default
title: Subscriptions over DLQ Topic
nav_order: 2
parent: Receiving messages
grand_parent: How to Guides
---

# Creating subscriptions over DLQ Topics

If the metro service attempts to deliver a message but the subscriber can't acknowledge it, after exhausting configured max_retries, metro forwards the undeliverable message to a dead-letter topic(DLQ). 

Metro also allows clients to create subscription over DLQ topics with certain limitations.

Creating a subscription over DLQ topic is similar to creating subscription over a normal topic. In the **topic** parameter in the payload, use the dlq-topic over which you want to create the subscription.

## Limitations
* Subscriptions over DLQ-topics will not have dead lettering support.
* This means, if the message delivery is not successful after max attempts, message will be lost.


Sample Subscription:
```json
{
    "topic": "projects/my-project/topics/mysub-dlq",
    "ackDeadlineSeconds": 90,
    "retryPolicy": {
        "minimumBackoff": "5s",
        "maximumBackoff": "30s"
    },
    "push_config": {
        "basic_auth": {
            "username": "<user>",
            "password": "<password>"
        },
        "push_endpoint" : "<push_endpoint>"
    },
    "deadLetterPolicy": {
        "maxDeliveryAttempts": 5
    },
    "filter": "<Filter Expression>"
}
```