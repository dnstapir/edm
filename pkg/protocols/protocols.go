package protocols

import (
	"time"

	"github.com/miekg/dns"
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

// Consts and content of bitsFromMsg() borrowed from miekg/dns, see
// https://github.com/miekg/dns/issues/1499
const (
	// Header.Bits
	_QR = 1 << 15 // query/response (response=1)
	_AA = 1 << 10 // authoritative
	_TC = 1 << 9  // truncated
	_RD = 1 << 8  // recursion desired
	_RA = 1 << 7  // recursion available
	_Z  = 1 << 6  // Z
	_AD = 1 << 5  // authenticated data
	_CD = 1 << 4  // checking disabled
)

func bitsFromMsg(dns *dns.Msg) uint16 {
	bits := uint16(dns.Opcode)<<11 | uint16(dns.Rcode&0xF) // #nosec G115 -- The Opcode and Rcode fields while ints in the dns struct represents only 4 bits each
	if dns.Response {
		bits |= _QR
	}
	if dns.Authoritative {
		bits |= _AA
	}
	if dns.Truncated {
		bits |= _TC
	}
	if dns.RecursionDesired {
		bits |= _RD
	}
	if dns.RecursionAvailable {
		bits |= _RA
	}
	if dns.Zero {
		bits |= _Z
	}
	if dns.AuthenticatedData {
		bits |= _AD
	}
	if dns.CheckingDisabled {
		bits |= _CD
	}

	return bits
}

func NewQnameEvent(msg *dns.Msg, ts time.Time) NewQnameJSON {
	bits := bitsFromMsg(msg)
	flags := int(bits)

	qType := int(msg.Question[0].Qtype)
	qClass := int(msg.Question[0].Qclass)

	return NewQnameJSON{
		Type:      NewQnameJSONType,
		Qname:     msg.Question[0].Name,
		Qtype:     &qType,
		Qclass:    &qClass,
		Timestamp: &ts,
		Flags:     &flags,
		Version:   NewQnameJSONVersion,
	}
}
