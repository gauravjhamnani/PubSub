package main

type Subscription struct {
	id string;
	subscriberFunc func(Message, string);
}

func NewSubscription(subId string) *Subscription {
	var subObj Subscription
	subObj.id = subId
	return &subObj
}