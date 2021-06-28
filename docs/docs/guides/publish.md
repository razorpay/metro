---
layout: default
title: Publishing messages
parent: How to Guides
nav_order: 1
---

Publishing messages to topics
---
A publisher application creates and sends messages to a topic. Metro offers at-least-once message delivery and best-effort ordering to the existing subscribers, as explained in the [Subscriber Overview](receiving/overview.md).

The general flow for a publisher application is:
- Create a message containing your data.
- Send a request to the Metro Server to publish the message to the desired topic.

Message format
---
A message consists of fields with the message data and metadata. Specify at least one of the following in the message:
- The message data
- An ordering key
- Attributes with additional metadata

If you're using the REST API, the message data must be base64-encoded.

The Metro service adds the following fields to the message:
- A message ID unique to the topic
- A timestamp for when the Metro service receives the message

Publishing messages
---
You can publish messages with the Metro API.

After you publish a message, the Metro service returns the message ID to the publisher.

Using attributes
---
You can embed custom attributes as metadata in Metro messages. Attributes can be text strings or byte strings. The message schema can be represented as follows:
```json
{
  "data": "<string>",
  "attributes": {
    "<key1 string>": "<value1 string>",
    "<key2 string>": "<value2 string>"
  },
  "messageId": "<string>",
  "publishTime": "<string>",
  "orderingKey": "<string>"
}
```

The `PubsubMessage` JSON schema is published as part of the REST and RPC documentation.
