package main

import (
	"time"
)

type Topic struct {
	id string;
	subscriptionList []*Subscription;
	createdOn time.Time;
	updatedOn time.Time;
}

func NewTopic(id string) *Topic {
	var topicObj Topic
	topicObj.subscriptionList = make([]*Subscription, 0)
	topicObj.createdOn = time.Now()
	topicObj.id = id
	return &topicObj
}