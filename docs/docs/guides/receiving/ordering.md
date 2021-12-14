---
layout: default
title: Ordering Messages
nav_order: 4
parent: Receiving messages
grand_parent: How to Guides
---
# Ordering Messages

Metro supports ordered delivery of messages. To enable ordered delivery of messages, an *Ordered Subscription* has to be created. The publisher has to publish messages with an *Ordering Key*.

## Ordered Subscription
To create an ordered subscription, the subscription has to be created with `enableMessageOrdering: true`.  

Sample Subscription:
```json
{
    "topic": "projects/my-project/topics/mytopic",
    "pushConfig": {
         "pushEndpoint": "https://www.mypushendpoint.com/path",
         "basicAuth": {
           "username": "api",
           "password": "mypassword"
         }
    },
    "ackDeadlineSeconds": 90,
    "retryPolicy": {
        "minimumBackoff": "5s",
        "maximumBackoff": "30s"
    },
    "deadLetterPolicy": {
        "maxDeliveryAttempts": 5
    },
	"enableMessageOrdering": true
}
```

## Ordering Key
To receive messages in order, the publisher has to publish messages with an *ordering key*.  

Sample Payload:
```json
{
    "messages": [
        {
            "data": "YQ==",
            "orderingKey": "o1"
        }
    ]
}
```
Messages having the same ordering key are delivered in the order Metro receives them. The order of messages delivered across multiple ordering keys can be different from the order in which they are published.

## At Least Once Delivery
Metro promises at least once delivery. Hence if a message is redelivered, Metro would redeliver all subsequent messages in the same ordering key to maintain order even if they have been acknowledged.

## Reduced Throughput
To maintain ordering, if a message with an ordering key is *nacked*, Metro would not deliver subsequent messages of the same ordering key until the message is either *acked* or pushed to DLQ.  



