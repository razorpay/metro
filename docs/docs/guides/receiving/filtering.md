---
layout: default
title: Subscriptions with filter
nav_order: 2
parent: Receiving messages
grand_parent: How to Guides
---

When you receive messages from a subscription with a filter, you only receive the messages that match the filter. Metro automatically acknowledges the messages that don't match the filter. You can filter message by their attributes.

# Creating subscriptions with filters

To create a subscription with filter capability, filtering expression need to be passed along with the payload while creating subscriptions in `filter` field.

Sample Subscription:
```json
{
    "topic": "projects/my-project/topics/mytopic",
    "ackDeadlineSeconds": 90,
    "retryPolicy": {
        "minimumBackoff": "5s",
        "maximumBackoff": "30s"
    },
    "deadLetterPolicy": {
        "maxDeliveryAttempts": 5
    },
	"filter": "<Filter Expression>"
}
```

Filter can be used in the similar manner while creating the push subscription also.

# Filtering syntax

To filter messages, write an expression that operates on attributes. You can write an expression that matches the key or value of the attributes. The **attributes** identifier selects the attributes in the message.

## Comparison operators

You can filter attributes with the following comparison operators:

* **:**
* **=**
* **!=**

The **:** operator matches a key in a list of attributes.

```
Example: `attributes:domain`
```


The equality operators i.e. **=** and **!=** match keys and values. The value must be a string literal.

```
Example: `attributes.domain = "com"` , `attributes.domain != "org"`
```

The key and value are case-sensitive and must match the attribute exactly. If a key contains characters other than hyphens, underscores, or alphanumeric characters, use a string literal.


## Boolean operators

* Boolean Operators `AND` and `OR` can be used to join multiple conditions. 
* `NOT` Operator can be used to negate the condition
* To combine the `AND` and `OR` operators, use parentheses and complete expressions.

**Examples**
```
- attributes:"iana.org/language_tag" AND attributes.domain = "com"

- attributes:"iana.org/language_tag" OR attributes.domain = "org" OR attributes.tag = "dot"

- NOT attributes:domain

- attributes:"iana.org/language_tag" AND (attributes.domain = "net" OR attributes.domain = "org")
```


## Functions

The `hasPrefix` function filters for attributes with values that start with a substring.

```
Example: hasPrefix(attributes.domain, "co")
```