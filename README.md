A PubSub system is a message propagation system that decouples senders and receivers.

The project is to create a PubSub system in Golang.

A PubSub system is a message propagation system that decouples senders and receivers.
Senders are called Publishers and Receivers are called Subscribers. These are the clients/users of the system.
There are two components in the system: Topics and Subscriptions. There is one to many mapping between Topics and Subscriptions.
Publisher publishes message to a Topic and Subscribers subscribe to Subscriptions to receive these messages. Any message sent to a Topic is propagated to all of its Subscriptions. There can be only one subscriber attached to a subscription.
Subscriptions are push based, they send the message to the subscriber instead of letting the subscriber pull.

The PubSub system supports below methods:
CreateTopic(topicID)
DeleteTopic(TopicID)
AddSubscription(topicID,SubscriptionID); Creates and adds subscription with id SubscriptionID to topicName.
DeleteSubscription(SubscriptionID)
Publish(topicID, message); publishes the message on given topic
Subscribe(SubscriptionID, SubscriberFunc); SubscriberFunc is the subscriber which is executed for each message of subscription.
UnSubscribe(SubscriptionID)
Ack(SubscriptionID, MessageID); Called by Subscriber to intimate the Subscription that the message has been received and processed.

Retry case a subscriber is not able to ack message within a time limit is also handled.
