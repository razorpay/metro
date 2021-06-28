---
layout: default
title: What is metro
parent: Concepts
nav_order: 1
---

Metro is an asynchronous messaging service that decouples services that produce events from services that process events. Metro offers durable message storage and real-time message delivery with high availability and consistent performance at scale.

Core concepts
---
<b>Project:</b> a named resource representing the namespace. all other resources are scoped to a project

<b>Topic:</b> A named resource to which messages are sent by publishers.

<b>Subscription:</b> A named resource representing the stream of messages from a single, specific topic, to be delivered to the subscribing application. For more details about subscriptions and message delivery semantics, see the Subscriber Guide.

<b>Message:</b> The combination of data and (optional) attributes that a publisher sends to a topic and is eventually delivered to subscribers.

<b>Message attribute:</b> A key-value pair that a publisher can define for a message. For example, key iana.org/language_tag and value en could be added to messages to mark them as readable by an English-speaking subscriber.

Publisher-subscriber relationships
---
A publisher application creates and sends messages to a topic. Subscriber applications create a subscription to a topic to receive messages from it. Communication can be one-to-many (fan-out), many-to-one (fan-in), and many-to-many, as the following diagram shows.

Metro message flow
---
1. A publisher application creates a topic in the Metro service and sends messages to the topic. A message contains a payload and optional attributes that describe the payload content.
2. The service ensures that published messages are retained on behalf of subscriptions. A published message is retained for a subscription until it is acknowledged by any subscriber consuming messages from that subscription.
3. Metro forwards messages from a topic to all of its subscriptions, individually.
4. A subscriber receives messages either by Metro pushing them to the subscriber's chosen endpoint, or by the subscriber pulling them from the service.
5. The subscriber sends an acknowledgement to the Metro service for each received message.
6. The service removes acknowledged messages from the subscription's message queue.

Publisher and subscriber endpoints
---
Publishers can be any application that can make HTTPS requests to metro.

Pull subscribers can also be any application that can make HTTPS requests to metro.

Push subscribers must be Webhook endpoints that can accept POST requests over HTTPS.
