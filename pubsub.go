package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

var lock sync.Mutex
var wg sync.WaitGroup

const (
	NUMBER_OF_RETRIES int = 2
)

type PubSub interface {
	CreateTopic(string)
	DeleteTopic(string)
	AddSubscription(string, string)
	DeleteSubscription(string)
	Publish(string, Message)
	Subscribe(string, func(Message, string))
	UnSubscribe(string)
	Ack(string, string)
}

type PubSubInMemory struct {
	topicIdTopicMap               map[string](*Topic)
	subscriptionIdTopicIdMap      map[string]string
	subscriptionIdsubscriptionMap map[string](*Subscription)
	ackResponseChannelMap         map[AckResponse](chan bool)
	messageCount                  int
}

func NewPubSubInMemory() *PubSubInMemory {
	var obj PubSubInMemory
	obj.topicIdTopicMap = make(map[string]*Topic)
	obj.subscriptionIdTopicIdMap = make(map[string]string)
	obj.subscriptionIdsubscriptionMap = make(map[string]*Subscription)
	obj.ackResponseChannelMap = make(map[AckResponse]chan bool)
	obj.messageCount = 0
	return &obj
}

func removeSubscriptionFromSlice(subscriptionList *[]*Subscription, subscriptionId string) {
	var index int
	for i, v := range *subscriptionList {
		if v.id == subscriptionId {
			index = i
			break
		}
	}
	(*subscriptionList)[index] = (*subscriptionList)[len((*subscriptionList))-1]
	*subscriptionList = (*subscriptionList)[:len((*subscriptionList))-1]
	fmt.Println(subscriptionList)
}

func (obj *PubSubInMemory) CreateTopic(topicId string) {
	fmt.Println("Create topic request with topicId : ", topicId)
	lock.Lock()
	defer lock.Unlock()
	_, isTopicAlreadyPresent := obj.topicIdTopicMap[topicId]
	if isTopicAlreadyPresent {
		fmt.Println("Topic already exists with Id", topicId)
	} else {
		obj.topicIdTopicMap[topicId] = NewTopic(topicId)
		fmt.Println("Topic successfully added with id", topicId)
	}
}

func (obj *PubSubInMemory) DeleteTopic(topicId string) {
	fmt.Println("Delete Topic request for topicId : ", topicId)
	lock.Lock()
	defer lock.Unlock()
	topicObjPtr, isTopicAlreadyPresent := obj.topicIdTopicMap[topicId]
	if !isTopicAlreadyPresent {
		fmt.Println("No topic found with given Id", topicId)
	} else {
		for _, subscriptionObj := range topicObjPtr.subscriptionList {
			delete(obj.subscriptionIdTopicIdMap, subscriptionObj.id)
		}
		delete(obj.topicIdTopicMap, topicId)
		fmt.Println("Successfully deleted topic with id :", topicId)
	}
}

func (obj *PubSubInMemory) AddSubscription(topicId, subscriptionId string) {
	fmt.Println("Add Subscription request for topicId :", topicId, "and subscriptionId", subscriptionId)
	lock.Lock()
	defer lock.Unlock()
	topicObjPtr, isTopicAlreadyPresent := obj.topicIdTopicMap[topicId]
	if !isTopicAlreadyPresent {
		fmt.Println("No topic found with given Id", topicId)
	} else {
		subObjPtr, isSubscriptionObjAlreadyPresent := obj.subscriptionIdsubscriptionMap[subscriptionId]
		if !isSubscriptionObjAlreadyPresent {
			fmt.Println("No Subscription registered with subscription Id :", subscriptionId, "Hence creating it.")
			subObjPtr = NewSubscription(subscriptionId)
			obj.subscriptionIdsubscriptionMap[subscriptionId] = subObjPtr
		}
		topicObjPtr.subscriptionList = append(topicObjPtr.subscriptionList, subObjPtr)
		topicObjPtr.updatedOn = time.Now()
		obj.subscriptionIdTopicIdMap[subscriptionId] = topicId
		fmt.Println("Successfully added new subscription Id :", subscriptionId, "to topicId :", topicId)
	}
}

func (obj *PubSubInMemory) DeleteSubscription(subscriptionId string) {
	fmt.Println("Delete Subscription request for subscriptionId :", subscriptionId)
	lock.Lock()
	defer lock.Unlock()
	_, isSubAlreadyPresent := obj.subscriptionIdsubscriptionMap[subscriptionId]
	if !isSubAlreadyPresent {
		fmt.Println("No subscription found with given sub id :", subscriptionId)
	} else {
		topicId := obj.subscriptionIdTopicIdMap[subscriptionId]
		removeSubscriptionFromSlice(&obj.topicIdTopicMap[topicId].subscriptionList, subscriptionId)
		fmt.Println(obj.topicIdTopicMap[topicId].subscriptionList)
		delete(obj.subscriptionIdTopicIdMap, subscriptionId)
		delete(obj.subscriptionIdsubscriptionMap, subscriptionId)
		fmt.Println("Successfully deleted subscription with id :", subscriptionId)
	}
}

func (obj *PubSubInMemory) Subscribe(subscriptionId string, subscriberFunc func(Message, string)) {
	fmt.Println("Subscribe Subscription request for subscriptionId :", subscriptionId)
	lock.Lock()
	defer lock.Unlock()
	subObjPtr, isSubAlreadyPresent := obj.subscriptionIdsubscriptionMap[subscriptionId]
	if !isSubAlreadyPresent {
		fmt.Println("No subscription found with given sub id :", subscriptionId)
	} else {
		subObjPtr.subscriberFunc = subscriberFunc
		fmt.Println("Successfully subscribed subscription with id :", subscriptionId)
	}
}

func (obj *PubSubInMemory) UnSubscribe(subscriptionId string) {
	fmt.Println("Unsubscribe Subscription request for subscriptionId :", subscriptionId)
	lock.Lock()
	defer lock.Unlock()
	subObjPtr, isSubAlreadyPresent := obj.subscriptionIdsubscriptionMap[subscriptionId]
	if !isSubAlreadyPresent {
		fmt.Println("No subscription found with given sub id :", subscriptionId)
	} else {
		subObjPtr.subscriberFunc = nil
		fmt.Println("Successfully unsubscribed subscription with id :", subscriptionId)
	}
}

func triggerMessagePropagation(subObj *Subscription, message *Message, messageId string, ch <-chan bool) {
	fmt.Println("Pulishing message", message.data, "to subsciptionId", subObj.id, ". Message ID :", messageId)
	count := NUMBER_OF_RETRIES
	for count >= 0 {
		select {
		case temp := <-ch:
			fmt.Println("Message Acknowldeged for messageId", messageId, "subsciptionId :", subObj.id, temp)
		default:
			fmt.Println("Triggering subscriber function for subsciptionId", subObj.id, "messageId :", messageId)
			subObj.subscriberFunc(*message, messageId)
			count--
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (obj *PubSubInMemory) Publish(topicId string, message Message) {
	fmt.Println("Publish message request in topicId :", topicId, "with message :", message.data)
	lock.Lock()
	defer lock.Unlock()
	topicObjPtr, isTopicAlreadyPresent := obj.topicIdTopicMap[topicId]
	if !isTopicAlreadyPresent {
		fmt.Println("No topic exists with Id", topicId)
	} else {
		messagedId := strconv.Itoa(obj.messageCount)
		fmt.Println("message id allocated for publish request :", messagedId)
		for _, subObj := range topicObjPtr.subscriptionList {
			if subObj.subscriberFunc != nil {
				ch := make(chan bool)
				obj.ackResponseChannelMap[*NewAckResponse(subObj.id, messagedId)] = ch
				wg.Add(1)
				go triggerMessagePropagation(subObj, &message, messagedId, ch)
			}
		}
	}
	wg.Wait()
	obj.messageCount++
}

func (obj *PubSubInMemory) Ack(subscriptionId, messageId string) {
	fmt.Println("Response acknowledged for subsciptionId :", subscriptionId, "messageId", messageId)
	ackObj := *NewAckResponse(subscriptionId, messageId)
	ch := obj.ackResponseChannelMap[ackObj]
	if ch != nil {
		wg.Done()  // this should be moved to triggerMessagePropagation but I was facing some wierd deadlock issue which I couldn't debug successfully because of time constraint
		ch <- true
		delete(obj.ackResponseChannelMap, ackObj)
	}
}

func main() {
	var obj PubSub = NewPubSubInMemory()
	topicId := "gaurav"
	sub1Id := "sub1"
	sub2Id := "sub2"
	message := *NewMessage(map[string]string{
		"key1": "value1",
	})
	message2 := *NewMessage(map[string]string{
		"key2": "value2",
	})
	obj.CreateTopic(topicId)
	obj.AddSubscription(topicId, sub1Id)
	dummyFunSub1 := func(message Message, messageId string) {
		fmt.Println("Message recieved", message.data, messageId)
		obj.Ack(sub1Id, messageId)
	}
	obj.Subscribe(sub1Id, dummyFunSub1)
	obj.AddSubscription(topicId, sub2Id)
	dummyFunSub2 := func(message Message, messageId string) {
		fmt.Println("Message recieved", message.data, messageId)
		obj.Ack(sub2Id, messageId)
	}
	obj.Subscribe(sub2Id, dummyFunSub2)
	obj.Publish(topicId, message)
	obj.UnSubscribe(sub1Id)
	obj.Publish(topicId, message2)
	time.Sleep(time.Millisecond * 10000)
}
