---
layout: default
title: Subscriber Overview
nav_order: 1
parent: Receiving messages
grand_parent: How to Guides
---

# Subscriber Overview

This document gives an overview of how subscriptions work in  Metro. For details on pull and push delivery subscriptions, see the [Pull Subscriber Guide](pull.md) and the [Push Subscriber Guide](push.md).

To receive messages published to a topic, you must create a subscription to that topic. Only messages published to the topic after the subscription is created are available to subscriber applications. The subscription connects the topic to a subscriber application that receives and processes messages published to the topic. A topic can have multiple subscriptions, but a given subscription belongs to a single topic.

## At-Least-Once delivery
Metro delivers each published message at least once for every subscription. There are some exceptions to this at-least-once behavior:

- By default, a message that cannot be delivered within the maximum retention time of 7 days is deleted and is no longer accessible. This typically happens when subscribers do not keep up with the flow of messages. Note that you can configure message retention duration (the range is from 10 minutes to 7 days).
- A message published before a given subscription was created will usually not be delivered for that subscription. Thus, a message published to a topic that has no subscription will not be delivered to any subscriber.

Once a message is sent to a subscriber, the subscriber should acknowledge the message. A message is considered outstanding once it has been sent out for delivery and before a subscriber acknowledges it. Metro will repeatedly attempt to deliver any message that has not been acknowledged. While a message is outstanding to a subscriber, however, Metro tries not to deliver it to any other subscriber on the same subscription. The subscriber has a configurable, limited amount of time -- known as the `ackDeadline` -- to acknowledge the outstanding message. Once the deadline passes, the message is no longer considered outstanding, and Metro will attempt to redeliver the message.

Typically, Metro delivers each message once and in the order in which it was published. However, messages may sometimes be delivered out of order or more than once. In general, accommodating more-than-once delivery requires your subscriber to be idempotent when processing messages.


## Pull or push delivery
A subscription can use either the pull or push mechanism for message delivery. You can change or configure the mechanism at any time.

### Pull subscription

In pull delivery, your subscriber application initiates requests to the Metro server to retrieve messages.

1. The subscribing application explicitly calls the pull method, which requests messages for delivery.
2. The Metro server responds with the message (or an error if the queue is empty) , and an ack ID.
3. The subscriber explicitly calls the acknowledge method, using the returned ack ID to acknowledge receipt.

### Push subscription
In push delivery, Metro initiates requests to your subscriber application to deliver messages.

1. The Metro server sends each message as an HTTPS request to the subscriber application at a pre-configured endpoint.
2. The endpoint acknowledges the message by returning an HTTP success status code. A non-success response indicates that the message should be resent.

Metro dynamically adjusts the rate of push requests based on the rate at which it receives success responses.

The following table offers some guidance in choosing the appropriate delivery mechanism for your application:

|Pull|Push|
|---|---|
|Large volume of messages (many more than 1/second).|Multiple topics that must be processed by the same webhook.
|
|Efficiency and throughput of message processing is critical.|App Engine Standard and Cloud Functions subscribers.
|

The following table compares pull and push delivery:

|	|Pull|Push|
|---|---|---|
|Endpoints|Any device on the internet that has authorized credentials is able to call the Metro API.|An HTTPS server with non-self-signed certificate accessible on the public web. The receiving endpoint may be decoupled from the Metro subscription, so that messages from multiple subscriptions may be sent to a single endpoint.|
|Load balancing|Multiple subscribers can make pull calls to the same "shared" subscription. Each subscriber will receive a subset of the messages.|The push endpoint can be a load balancer.|
|Configuration|No configuration is necessary.|No configuration is necessary for App Engine apps in the same project as the subscriber.</br>Verification of push endpoints is not required in the Google Cloud Console. Endpoints must be reachable via DNS names and have SSL certificates installed.|
|Flow control|The subscriber client controls the rate of delivery.The subscriber can dynamically modify the ack deadline, allowing message processing to be arbitrarily long.|The Metro server automatically implements flow control. There is no need to handle message flow at the client side, although it is possible to indicate that the client cannot handle the current message load by passing back an HTTP error.|
|Efficiency and throughput|Achieves high throughput at low CPU and bandwidth by allowing batched delivery and acknowledgments as well as massively parallel consumption. May be inefficient if aggressive polling is used to minimize message delivery time.|Delivers one message per request and limits maximum number of outstanding messages.|
