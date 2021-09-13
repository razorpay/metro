---
layout: default
title: Receiving messages using Push
nav_order: 3
parent: Receiving messages
grand_parent: How to Guides
---

# Receiving messages using Push

Metro supports both push and pull message delivery. For an overview and comparison of pull and push subscriptions, see the [Subscriber Overview](overview.md). This document describes push delivery. For a discussion of pull delivery, see the [Pull Subscriber Guide](pull.md).

If a subscription uses push delivery, the Metro service delivers messages to a push endpoint. The push endpoint must be a publicly accessible HTTPS address. The server for the push endpoint must have a valid SSL certificate signed by a certificate authority.

In addition, push subscriptions can be configured to provide an authorization header to allow the endpoints to authenticate the requests.

## Receiving messages
When Metro delivers a message to a push endpoint, Metro sends the message in the body of a `POST` request. The body of the request is a JSON object and the message data is in the message.data field. The message data is base64-encoded.

The following example is the body of a `POST` request to a push endpoint:

```json
{
    "message": {
        "attributes": {
            "key": "value"
        },
        "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
        "messageId": "2070443601311540",
        "message_id": "2070443601311540",
        "publishTime": "2021-02-26T19:13:55.749Z",
        "publish_time": "2021-02-26T19:13:55.749Z",
    },
   "subscription": "projects/myproject/subscriptions/mysubscription",
   "deliveryAttempt": 1
}
```

To receive messages from push subscriptions, use a webhook and process the `POST` requests that Metro sends to the push endpoint.

After you receive a push request, return an HTTP status code. To acknowledge the message, return one of the following status codes:

* 102
* 200
* 201
* 202
* 204

To send a negative acknowledgement for the message, return any other status code. If you send a negative acknowledgement or the acknowledgement deadline expires, Metro resends the message. You can't modify the acknowledgement deadline of individual messages that you receive from push subscriptions.

## Stopping and resuming delivery

To temporarily stop Metro from sending requests to the push endpoint, change the subscription to pull. Note that it can take several minutes for this changeover to take effect.

To resume push delivery, set the URL to a valid endpoint again. To permanently stop delivery, delete the subscription.

## Quotas, limits and delivery rate
Note that push subscriptions are subject to a set of quotas and resource limits.

### Push backoff
If a push subscriber sends negative acknowledgements, Metro might deliver messages using a push backoff. When Metro uses a push backoff, it stops delivering messages for 100 milliseconds to 60 seconds and then starts delivering messages again.

The push backoff is an exponential backoff that prevents a push subscriber from receiving messages that it can't process. The amount of time that Metro stops delivering messages depends on the number of negative acknowledgments that push subscribers send.

For example, if a push subscriber receives five messages per second and sends one negative acknowledgment per second, Metro delivers messages approximately every 500 milliseconds. If the push subscriber sends five negative acknowledgment per second, Metro delivers messages every 30-60 seconds.

### Delivery rate
