---
layout: default
title: Receiving messages using Pull
nav_order: 2
parent: Receiving messages
grand_parent: How to Guides
---

# Receiving messages using Pull
Metro supports both push and pull message delivery. For an overview and comparison of pull and push subscriptions, see the [Subscriber Overview](overview.md). This document describes pull delivery. For a discussion of push delivery, see the [Push Subscriber Guide](push.md).

## Asynchronous Pull
Using asynchronous pulling provides higher throughput in your application, by not requiring your application to block for new messages. Messages can be received in your application using a long running message listener, and acknowledged one message at a time, as shown in the example below. Java, Python, .NET, Go, and Ruby clients use the StreamingPull service API to implement the asynchronous client API efficiently.

Not all client libraries support asynchronously pulling messages. To learn about synchronously pulling messages, see Synchronous Pull.

For more information, see the API Reference documentation in your programming language.

## Processing Custom Attributes
This sample shows how to pull messages asynchronously and retrieve the custom attributes from metadata:


## Listening for Errors
This sample shows how to handle errors that arise when subscribing to messages:

## Message Flow Control
Your subscriber client might process and acknowledge messages more slowly than Metro sends them to the client. In this case:

* It's possible that one client could have a backlog of messages because it doesn't have the capacity to process the volume of incoming messages, but another client on the network does have that capacity. The second client could reduce the subscription's backlog, but it doesn't get the chance to do so because the first client maintains a lease on the messages that it receives. This reduces the overall rate of processing because messages get stuck on the first client.

* Because the client library repeatedly extends the acknowledgement deadline for backlogged messages, those messages continue to consume memory, CPU, and bandwidth resources. As such, the subscriber client might run out of resources (such as memory). This can adversely impact the throughput and latency of processing messages.

To mitigate the issues above, use the flow control features of the subscriber to control the rate at which the subscriber receives messages. These flow control features are illustrated in the following samples:

More generally, the need for flow control indicates that messages are being published at a higher rate than they are being consumed. If this is a persistent state, rather than a transient spike in message volume, consider increasing the number of subscriber client instances.

## StreamingPull
The Metro service has two APIs for retrieving messages:
* [Pull]()
* [Streaming Pull]()

Where possible, the Cloud Client libraries use [StreamingPull](https://grpc.io/docs/what-is-grpc/core-concepts/#server-streaming-rpc) for maximum throughput and lowest latency. Although you might never use the StreamingPull API directly, it is important to understand some crucial properties of StreamingPull and how it differs from the more traditional Pull method.

The Pull method relies on a request/response model:

1. The client sends a request to the server for messages.
2. The server replies with zero or more messages and closes the connection.

The StreamingPull service API relies on a persistent bidirectional connection to receive multiple messages as they become available:
1. The client sends a request to the server to establish a connection.
2. The server continuously sends messages to the connected client.
3. The connection is eventually closed either by the client or the server.

You provide a callback to the subscriber and the subscriber asynchronously runs the callback for each message. If a subscriber receives messages with the same ordering key, the client libraries sequentially run the callback. The Metro service delivers these messages to the same subscriber on a best-effort basis.

#### StreamingPull has a 100% error rate (this is to be expected)
StreamingPull streams always close with a non-OK status. Note that, unlike in regular RPCs, the status here is simply an indication that the stream has been broken, not that requests are failing. Therefore, while the StreamingPull API may have a seemingly surprising 100% error rate, this is by design.

#### StreamingPull: Dealing with large backlogs of small messages
The gRPC StreamingPull stack is optimized for high throughput and therefore buffers messages. This can have some consequences if you are attempting to process large backlogs of small messages (rather than a steady stream of new messages). Under these conditions, you may see messages delivered multiple times and they may not be load balanced effectively across clients.

The buffer between the Metro service and the client library user space is roughly 10MB. To understand the impact of this buffer on client library behavior, consider this example:

* There is a backlog of 10,000 1KB messages on a subscription.
* Each message takes 1 second to process sequentially, by a single-threaded client instance.
* The first client instance to establish a StreamingPull connection to the service for that subscription will fill its buffer with all 10,000 messages.
* It takes 10,000 seconds (almost 3 hours) to process the buffer.
* In that time, some of the buffered messages exceed their acknowledgement deadline and are re-sent to the same client, resulting in duplicates.
* When multiple client instances are running, the messages stuck in the one client's buffer will not be available to any client instances.

This situation will not occur if you use [flow control](#message-flow-control) for StreamingPull: the service never has the entire 10MB of messages at a time and so is able to effectively load balance messages across multiple subscribers.

To address this situation, either use a push subscription or the Pull API, currently available in some of the Cloud Client Libraries (see the Synchronous Pull section) and all API Client libraries.

## Synchronous Pull
There are cases when the asynchronous Pull is not a perfect fit for your application. For example, the application logic might rely on a polling pattern to retrieve messages or require a precise cap on a number of messages retrieved by the client at any given time. To support such applications, the service supports a synchronous Pull method.

Metro delivers a list of messages. If the list has multiple messages, Metro orders the messages with the same ordering key.

Note that to achieve low message delivery latency with synchronous pull, it is important to have many simultaneously outstanding pull requests. As the throughput of the topic increases, more pull requests are necessary. In general, [asynchronous pull](#asynchronous-pull) is preferable for latency-sensitive applications.


