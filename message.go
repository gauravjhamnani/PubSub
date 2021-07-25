package main

type Message struct {
	data map[string]string;
}

func NewMessage(mapping map[string]string) *Message {
	var obj Message
	obj.data = mapping
	return &obj
}