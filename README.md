go-pubsub
=========

A simple abstraction for message publishing and consumption that presents a uniform API, regardless of the underlying implementation.

Current implementations and their status
----------------------------------------

| Implementation                           | Status        |
| ---------------------------------------- | ------------- |
| Mock (in memory testing implementation)  | incomplete    |
| Apache Kafka                             | beta          |
| Nats                                     | incomplete    |
| Nats streaming                           | beta          |
| AMQP                                     | incomplete    |
| AWS SQS                                  | beta          |
| AWS SNS                                  | beta          |


The API is not yet guaranteed but changes should be minimal from now.

