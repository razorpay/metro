---
layout: default
title: Subscriptions with filter
nav_order: 2
parent: Receiving messages
grand_parent: How to Guides
---

When you receive messages from a subscription with a filter, you only receive the messages that match the filter. Metro filters messages that match the filtering criteria before delivery. You can filter message by their attributes.

# Creating subscriptions with filters

To create a subscription with filtering, specify the `filter` in the request body.

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

Filtering can be used in the similar manner while creating the push subscription as well.

# Filtering syntax

To filter messages, write an expression that operates on attributes. You can write an expression that matches the key or value of the attributes. The **attributes** identifier provides access to the attributes sent as part of a message.

The key and value are case-sensitive and must match the attribute exactly. If a key contains characters other than hyphens, underscores, or alphanumeric characters, use a string literal. To use backslashes, quotation marks, and non-printing characters in a filter, escape the characters within a string literal. You can also use Unicode, hexadecimal, and octal escape sequences within a string literal.

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
* `NOT` Operator can be used to negate the condition.
* To combine the `AND` and `OR` operators, use parentheses and complete expressions.

**Examples**
```
- attributes:"iana.org/language_tag" AND attributes.domain = "com"

- attributes:"iana.org/language_tag" OR attributes.domain = "org" OR attributes.tag = "dot"

- NOT attributes:"\u307F\u3093\u306A"

- attributes:"iana.org/language_tag" AND (attributes.domain = "net" OR attributes.domain = "org")
```


## Functions

The `hasPrefix` function filters for attributes with values that start with a substring.

**Examples**
```
- hasPrefix(attributes.domain, "co")

- attributes:"iana.org/language_tag" AND (attributes.domain = "net" OR hasPrefix(attributes.domain, "co")) AND NOT hasPrefix(attributes.head, "foo")

- attributes.domain != "com" OR attributes.language_tag = "en_US" OR (NOT hasPrefix(attributes.head, "moo") OR attributes.head = "moot" )
```