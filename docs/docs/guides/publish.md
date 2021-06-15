---
layout: default
title: Publishing messages
parent: How to Guides
nav_order: 1
---

Publishing messages to topics
---
This document provides information about publishing messages.

A publisher application creates and sends messages to a topic. Pub/Sub offers at-least-once message delivery and best-effort ordering to existing subscribers, as explained in the Subscriber Overview.

The general flow for a publisher application is:
- Create a message containing your data.
- Send a request to the Pub/Sub Server to publish the message to the desired topic.
