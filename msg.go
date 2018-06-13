package main

// Msg interface
type Msg interface {
	// Topic get topic string
	Topic() string
	// SetTopic set topic string
	SetTopic(string)
	// Payload get payload byte array
	Payload() []byte
}

// msgType internal message type with topic and payload only
type msgType struct {
	topic   string
	payload []byte
}

// NewMsg create new msg instance
func NewMsg(topic string, payload []byte) Msg {
	return &msgType{
		topic:   topic,
		payload: payload,
	}
}

// Topic get topic string
func (m *msgType) Topic() string {
	return m.topic
}

// SetTopic set topic string
func (m *msgType) SetTopic(s string) {
	m.topic = s
}

// Payload get payload byte array
func (m *msgType) Payload() []byte {
	return m.payload
}
