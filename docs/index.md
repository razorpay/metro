---
layout: default
title: Overview
nav_order: 1
---

Metro facilitates communication, integration and orchestration between services in a microservices environment.

Benefits
---
- Scalable, in-order message delivery with pull and push modes
- Push delivery removes need of running separate workers with each microservice
- Filtering, dead-letter delivery, and exponential backoff without sacrificing scale help simplify your applications
- Single integration to solve all usecases

Features
---

| Name | Description |
| --- | --- |
|At-least-once delivery|Synchronous, per-message receipt tracking ensures at-least-once delivery at any scale.|
|Push Subscribers|In addition to the pull based subscribers, metro supports webhook based messages delivery|
|Message Ack/Nack|supports both Ack and Nack on a message allowing faster circuit breaking|
|Retry|Retry configuration allowsfor messages to be retried in case subscriber application were not able to process the delivery attempt|
|Dead Letter Queue|Dead letter topics allow for messages unable to be processed by subscriber applications to be put aside for offline examination and debugging so that other messages can be processed without delay.|
|Seek and replay|Rewind your backlog to any offset in past, giving the ability to reprocess the messages. Fast forward to discard outdated data.|
|Message Retention|Messages are retained as defined by subscriber, this defines the replay limitation|
|Ordering|Supports ordered delivery of the published messages|
|Filtering|Metro can filter messages based upon attributes in order to reduce delivery volumes to subscribers.|
|Schema Registry|Supports schema registry and validation to enforce contracts|
