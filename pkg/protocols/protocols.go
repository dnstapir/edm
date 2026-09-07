package protocols

import (
	"time"

	"github.com/dnstapir/dnswire"
)

// Implements https://github.com/dnstapir/protocols/blob/main/events/new_qname.yaml
type NewQnameJSON struct {
	// Flag Field (QR/Opcode/AA/TC/RD/TA/Z/RCODE)
	Flags *int `json:"flags,omitempty"`

	// Initiator corresponds to the JSON schema field "initiator".
	Initiator *NewQnameJSONInitiator `json:"initiator,omitempty"`

	// MessageId corresponds to the JSON schema field "message_id".
	MessageID *string `json:"message_id,omitempty"`

	// Query Class
	Qclass *int `json:"qclass,omitempty"`

	// Query Name
	Qname string `json:"qname"`

	// Query Type
	Qtype *int `json:"qtype,omitempty"`

	// Rdlength corresponds to the JSON schema field "rdlength".
	Rdlength *int `json:"rdlength,omitempty"`

	// Timestamp corresponds to the JSON schema field "timestamp".
	Timestamp *time.Time `json:"timestamp,omitempty"`

	// Type corresponds to the JSON schema field "type".
	Type NewQnameJSONTypeConst `json:"type"`

	// Version corresponds to the JSON schema field "version".
	Version int `json:"version"`
}

type (
	NewQnameJSONInitiator string
	NewQnameJSONTypeConst string
)

const (
	NewQnameJSONType              NewQnameJSONTypeConst = "new_qname"
	NewQnameJSONInitiatorClient   NewQnameJSONInitiator = "client"
	NewQnameJSONInitiatorResolver NewQnameJSONInitiator = "resolver"
	NewQnameJSONVersion                                 = 0
)

// NewQnameEvent constructs a [NewQnameJSON] event from a [dnswire.Message].
//
// Qname uses RFC presentation format. If the Question section is empty the
// returned event has an empty Qname, nil Qtype, and nil Qclass, but other
// header-derived fields are populated.
func NewQnameEvent(msg *dnswire.Message, ts time.Time) NewQnameJSON {
	event := NewQnameJSON{
		Type:      NewQnameJSONType,
		Timestamp: &ts,
		Flags:     new(int(msg.Header.Flags)),
		Version:   NewQnameJSONVersion,
	}

	if msg.Header.QuestionCount > 0 {
		event.Qname = msg.Question.Name
		event.Qtype = new(int(msg.Question.Type))
		event.Qclass = new(int(msg.Question.Class))
	}

	return event
}
