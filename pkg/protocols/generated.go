// Code generated by github.com/atombender/go-jsonschema, DO NOT EDIT.

package protocols

import "time"

type DomainName string

type EventsMqttMessageNewQnameJson struct {
	// Flag Field (QR/Opcode/AA/TC/RD/TA/Z/RCODE)
	Flags *int `json:"flags,omitempty"`

	// Initiator corresponds to the JSON schema field "initiator".
	Initiator *EventsMqttMessageNewQnameJsonInitiator `json:"initiator,omitempty"`

	// Query Class
	Qclass *int `json:"qclass,omitempty"`

	// Query Name
	Qname DomainName `json:"qname"`

	// Query Type
	Qtype *int `json:"qtype,omitempty"`

	// Rdlength corresponds to the JSON schema field "rdlength".
	Rdlength *int `json:"rdlength,omitempty"`

	// Timestamp corresponds to the JSON schema field "timestamp".
	Timestamp time.Time `json:"timestamp"`

	// Type corresponds to the JSON schema field "type".
	Type string `json:"type"`

	// Version corresponds to the JSON schema field "version".
	Version int `json:"version"`
}

type EventsMqttMessageNewQnameJsonInitiator string

const EventsMqttMessageNewQnameJsonInitiatorClient EventsMqttMessageNewQnameJsonInitiator = "client"
const EventsMqttMessageNewQnameJsonInitiatorResolver EventsMqttMessageNewQnameJsonInitiator = "resolver"
