package dnstap

import (
	"encoding/binary"
	"fmt"
	"math"
	"net/netip"
	"time"
)

type Flag uint64

const (
	DNSTAP_TYPE Flag = 1 << iota
	MESSAGE
	MESSAGE_TYPE
	DNS_MESSAGE

	IDENTITY
	VALID_SOCKET_PROTOCOL
	VALID_QUERY_ADDR
	VALID_RESPONSE_ADDR
	VALID_QUERY_PORT
	VALID_RESPONSE_PORT
	VALID_TIME
	has_QUERY_TIME
	has_RESPONSE_TIME

	VALID Flag = DNSTAP_TYPE | MESSAGE | MESSAGE_TYPE | DNS_MESSAGE | VALID_QUERY_ADDR | VALID_RESPONSE_ADDR | VALID_QUERY_PORT | VALID_RESPONSE_PORT | VALID_TIME
)

type Message struct {
	Flags          Flag
	IsQuery        bool
	SocketProtocol uint8
	QueryPort      uint16
	ResponsePort   uint16
	Identity       string
	QueryAddr      netip.Addr
	ResponseAddr   netip.Addr
	Timestamp      time.Time
	Message        []byte
}

type meta struct {
	queryTimeSec     uint64
	responseTimeSec  uint64
	queryTimeNSec    uint32
	responseTimeNSec uint32
	queryMessage     []byte
	responseMessage  []byte
	socketFamily     uint8
}

func (m *Message) Unpack(data []byte) (err error) {
	// catch panics and return them as errors
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("parsing error: %v", r)
		}
	}()
	// reset message
	*m = Message{}
	// create meta data object
	meta := meta{}
	// unpack
	m.unpackDnsTap(data, &meta)
	// return
	return nil
}

func (m *Message) HasFlags(flags Flag) bool {
	return m.Flags&flags == flags
}

func (m *Message) unpackDnsTap(data []byte, meta *meta) {
	tlv := uint8(0)
	valid := false

	for len(data) > 0 {
		// extract TLV
		tlv, valid, data = getTLV(data)
		// if not valid => skip TLV
		if !valid {
			data = skipType(tlv, data)
			continue
		}
		// switch based on field
		switch tlv >> 3 {
		case 1:
			assertType(protoLEN, tlv)
			m.Flags |= IDENTITY
			m.Identity, data = readLEN[string](data)
		case 14:
			assertType(protoLEN, tlv)
			m.Flags |= MESSAGE
			var msg []byte
			msg, data = readLEN[[]byte](data)
			m.unpackMessage(msg, meta)
		case 15:
			assertType(protoVARINT, tlv)
			m.Flags |= DNSTAP_TYPE
			var t uint64
			t, data = readVARINT(data)
			if t != 1 {
				panic("Dnstap field Type is not MESSAGE")
			}
		default:
			data = skipType(tlv, data)
		}
	}
	// some validations checks
	if !m.HasFlags(DNSTAP_TYPE | MESSAGE | MESSAGE_TYPE) {
		if !m.HasFlags(DNSTAP_TYPE) {
			panic("Dnstap field Type is not present")
		}
		if !m.HasFlags(MESSAGE) {
			panic("Dnstap missing Message")
		}
		if !m.HasFlags(MESSAGE_TYPE) {
			panic("Dnstap missing Message Type")
		}
	}
	// make sure ip addresses matches the defined socket family
	switch meta.socketFamily {
	case 1: // IPv4
		if !m.QueryAddr.Is4() {
			m.QueryAddr = netip.Addr{}
			m.Flags &= ^VALID_QUERY_ADDR
		}
		if !m.ResponseAddr.Is4() {
			m.ResponseAddr = netip.Addr{}
			m.Flags &= ^VALID_RESPONSE_ADDR
		}
	case 2: // IPv6
		if !m.QueryAddr.Is6() {
			m.QueryAddr = netip.Addr{}
			m.Flags &= ^VALID_QUERY_ADDR
		}
		if !m.ResponseAddr.Is6() {
			m.ResponseAddr = netip.Addr{}
			m.Flags &= ^VALID_RESPONSE_ADDR
		}
	default: // not IPv4 nor IPv6
		// unkown socket family => reset addresses
		m.QueryAddr = netip.Addr{}
		m.Flags &= ^VALID_QUERY_ADDR
		m.ResponseAddr = netip.Addr{}
		m.Flags &= ^VALID_RESPONSE_ADDR
	}
	// arrange data
	switch m.IsQuery {
	case true:
		if meta.queryMessage != nil {
			m.Flags |= DNS_MESSAGE
		}
		m.Message = meta.queryMessage
		if m.HasFlags(has_QUERY_TIME) {
			if meta.queryTimeSec <= math.MaxInt64 {
				m.Flags |= VALID_TIME
			}
		}
		m.Timestamp = toTime(meta.queryTimeSec, meta.queryTimeNSec)
	case false:
		if meta.responseMessage != nil {
			m.Flags |= DNS_MESSAGE
		}
		m.Message = meta.responseMessage
		if m.HasFlags(has_RESPONSE_TIME) {
			if meta.responseTimeSec <= math.MaxInt64 {
				m.Flags |= VALID_TIME
			}
		}
		m.Timestamp = toTime(meta.responseTimeSec, meta.responseTimeNSec)
	}
}

func (m *Message) unpackMessage(data []byte, meta *meta) {
	var buf []byte

	tlv := uint8(0)
	valid := false

	for len(data) > 0 {
		// extract TLV
		tlv, valid, data = getTLV(data)
		// if not valid => skip TLV
		if !valid {
			data = skipType(tlv, data)
			continue
		}
		// switch based on field
		switch tlv >> 3 {
		case 1:
			assertType(protoVARINT, tlv)
			m.Flags |= MESSAGE_TYPE
			var t uint64
			t, data = readVARINT(data)
			switch t {
			case 1, 3, 5, 7, 9, 11, 13:
				m.IsQuery = true
			case 2, 4, 6, 8, 10, 12, 14:
				m.IsQuery = false
			default:
				panic("Message Type unknown")
			}
		case 2:
			assertType(protoVARINT, tlv)
			var family uint64
			family, data = readVARINT(data)
			switch family > math.MaxUint8 {
			case true:
				// Socket Family too large
				meta.socketFamily = 0
			case false:
				meta.socketFamily = uint8(family & math.MaxUint8)
			}
		case 3:
			assertType(protoVARINT, tlv)
			var protocol uint64
			protocol, data = readVARINT(data)
			switch protocol > math.MaxUint8 {
			case true:
				// Socket Protocol too large
				m.Flags &= ^VALID_SOCKET_PROTOCOL
				m.SocketProtocol = 0
			case false:
				m.Flags |= VALID_SOCKET_PROTOCOL
				m.SocketProtocol = uint8(protocol & math.MaxUint8)
			}
		case 4:
			assertType(protoLEN, tlv)
			buf, data = readLEN[[]byte](data)
			addr, ok := netip.AddrFromSlice(buf)
			switch ok {
			case true:
				// Valid Query Address
				m.Flags |= VALID_QUERY_ADDR
			case false:
				// Invalid Query Address
				m.Flags &= ^VALID_QUERY_ADDR
			}
			m.QueryAddr = addr
		case 5:
			assertType(protoLEN, tlv)
			var buf []byte
			buf, data = readLEN[[]byte](data)
			addr, ok := netip.AddrFromSlice(buf)
			switch ok {
			case true:
				// Valid Response Address
				m.Flags |= VALID_RESPONSE_ADDR
			case false:
				// Invalid Response Address
				m.Flags &= ^VALID_RESPONSE_ADDR
			}
			m.ResponseAddr = addr
		case 6:
			assertType(protoVARINT, tlv)
			var port uint64
			port, data = readVARINT(data)
			switch port > math.MaxUint16 {
			case true:
				// Query Port too large
				m.Flags &= ^VALID_QUERY_PORT
				m.QueryPort = 0
			case false:
				m.Flags |= VALID_QUERY_PORT
				m.QueryPort = uint16(port & math.MaxUint16)
			}
		case 7:
			assertType(protoVARINT, tlv)
			var port uint64
			port, data = readVARINT(data)
			switch port > math.MaxUint16 {
			case true:
				// Response Port too large
				m.Flags &= ^VALID_RESPONSE_PORT
				m.ResponsePort = 0
			case false:
				m.Flags |= VALID_RESPONSE_PORT
				m.ResponsePort = uint16(port & math.MaxUint16)
			}
		case 8:
			assertType(protoVARINT, tlv)
			m.Flags |= has_QUERY_TIME
			meta.queryTimeSec, data = readVARINT(data)
		case 9:
			assertType(protoI32, tlv)
			meta.queryTimeNSec, data = readUint32(data)
		case 10:
			assertType(protoLEN, tlv)
			meta.queryMessage, data = readLEN[[]byte](data)
		case 12:
			assertType(protoVARINT, tlv)
			m.Flags |= has_RESPONSE_TIME
			meta.responseTimeSec, data = readVARINT(data)
		case 13:
			assertType(protoI32, tlv)
			meta.responseTimeNSec, data = readUint32(data)
		case 14:
			assertType(protoLEN, tlv)
			meta.responseMessage, data = readLEN[[]byte](data)
		default:
			data = skipType(tlv, data)
		}
	}
}

func getTLV(data []byte) (uint8, bool, []byte) {
	// extract tlv
	tlv := data[0]
	// check that the tlv isn't longer than one byte
	if tlv&0b10000000 != 0 {
		// ids larger than 15 not supported
		// => jumping over its VARINT
		return tlv | 0b11111000, false, skipVARINT(data)
	}
	// tlv is valid
	return tlv, true, data[1:]
}

func toTime(sec uint64, nsec uint32) time.Time {
	if sec > math.MaxInt64 {
		return time.Unix(0, 0).UTC()
	}
	return time.Unix(int64(sec), int64(nsec)).UTC()
}

type protoType uint8

const (
	protoVARINT protoType = 0b000
	protoI64    protoType = 0b001
	protoLEN    protoType = 0b010
	protoSGROUP protoType = 0b011
	protoEGROUP protoType = 0b100
	protoI32    protoType = 0b101
)

func assertType(t protoType, tlv uint8) {
	if protoType(tlv&0b00000111) != t {
		panic("incorrect type")
	}
}

func skipType(tlv uint8, data []byte) []byte {
	switch protoType(tlv & 0b00000111) {
	case protoVARINT:
		return skipVARINT(data)
	case protoI64:
		return data[8:]
	case protoLEN:
		l, data := readVARINT(data)
		return data[l:]
	case protoI32:
		return data[4:]
	default:
		panic("unknown type")
	}
}

func readUint32(data []byte) (uint32, []byte) {
	n := binary.LittleEndian.Uint32(data)
	return n, data[4:]
}

func readLEN[T string | []byte](data []byte) (T, []byte) {
	l, data := readVARINT(data)
	return T(data[0:l]), data[l:]
}

func readVARINT(data []byte) (uint64, []byte) {
	out := uint64(0)
	n := byte(0)

	for i := range 10 {
		// extract next byte
		n = data[0]
		// advance data
		data = data[1:]
		// update out
		out = out | uint64(n&0b01111111)<<(i*7)
		// return if no continuation bit
		if n&0b10000000 == 0 {
			return out, data
		}
	}

	panic("VARINT overflow")
}

func skipVARINT(data []byte) []byte {
	for i := range 10 {
		// return if no continuation bit
		if data[i]&0b10000000 == 0 {
			return data[i+1:]
		}
	}
	panic("VARINT overflow")
}
