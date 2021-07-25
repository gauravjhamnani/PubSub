package main

type AckResponse struct {
	subsciptionId string
	messageId string
}

func NewAckResponse(subsciptionId, messageId string) *AckResponse {
	var ackObj AckResponse
	ackObj.messageId = messageId
	ackObj.subsciptionId = subsciptionId
	return &ackObj
}