package dnstap

import (
	"bytes"
	"fmt"
	"math"
	"net/netip"
	"reflect"
	"slices"
	"testing"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"google.golang.org/protobuf/proto"
)

var (
	mpa  = netip.MustParseAddr
	mpas = func(addr string) []byte { return mpa(addr).AsSlice() }
)

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Error not expected: %v", err)
	}
}

func mustErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Error expected but got nil")
	}
}

func TestUnpack(t *testing.T) {
	t.Run("check Dnstap.Type", func(t *testing.T) {
		_, err := doUnpack(t, &dnstap.Dnstap{
			Type: new(dnstap.Dnstap_Type(2)),
		})
		mustErr(t, err)
	})

	t.Run("check Dnstap.Message", func(t *testing.T) {
		_, err := doUnpack(t, &dnstap.Dnstap{
			Type: new(dnstap.Dnstap_MESSAGE),
		})
		mustErr(t, err)
	})

	t.Run("check Dnstap.Message.Type", func(t *testing.T) {
		_, err := doUnpack(t, &dnstap.Dnstap{
			Type: new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type: new(dnstap.Message_Type(math.MinInt32)),
			},
		})
		mustErr(t, err)
	})

	t.Run("valid minimal", func(t *testing.T) {
		dt, err := doUnpack(t, &dnstap.Dnstap{
			Type: new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type: new(dnstap.Message_AUTH_QUERY),
			},
		})
		noErr(t, err)
		checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType)
	})

	t.Run("valid minimal (manual)", func(t *testing.T) {
		dt, err := doUnpack(t, []byte{
			15<<3 | uint8(protoVARINT), 1, // dnstap.Type = Message
			14<<3 | uint8(protoLEN), 2, // dnstap.Message
			1<<3 | uint8(protoVARINT), 1, // dnstap.Message.Type = AUTH_QUERY
		})
		noErr(t, err)
		checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType)
	})

	t.Run("reset upon Unpack", func(t *testing.T) {
		// create a populated struct
		dt := Message{
			flags:          Flag(math.MaxUint64),
			IsQuery:        true,
			SocketProtocol: math.MaxUint8,
			QueryPort:      math.MaxUint16,
			ResponsePort:   math.MaxUint16,
			Identity:       "Test Identity",
			QueryAddr:      mpa("192.0.2.1"),
			ResponseAddr:   mpa("192.0.2.2"),
			Timestamp:      time.Now(),
			Message:        []byte("DNS"),
		}
		// do a unpack
		noErr(t, dt.Unpack([]byte{
			15<<3 | uint8(protoVARINT), 1, // dnstap.Type = Message
			14<<3 | uint8(protoLEN), 2, // dnstap.Message
			1<<3 | uint8(protoVARINT), 1, // dnstap.Message.Type = AUTH_QUERY
		}))
		checkFlags(t, &dt, WithDnstapType|WithMessage|WithMessageType)
		// verify fields
		expected := Message{
			flags:     WithDnstapType | WithMessage | WithMessageType,
			IsQuery:   true,
			Timestamp: time.Unix(0, 0).UTC(),
		}
		if !reflect.DeepEqual(expected, dt) {
			t.Fatalf("struct not as expected")
		}
	})

	t.Run("no dnstap.Type", func(t *testing.T) {
		_, err := doUnpack(t, []byte{
			14<<3 | uint8(protoLEN), 2, // dnstap.Message
			1<<3 | uint8(protoVARINT), 1, // dnstap.Message.Type = AUTH_QUERY
		})
		mustErr(t, err)
	})

	t.Run("no dnstap.Message", func(t *testing.T) {
		_, err := doUnpack(t, []byte{
			15<<3 | uint8(protoVARINT), 1, // dnstap.Type = Message
		})
		mustErr(t, err)
	})

	t.Run("no dnstap.Message.Type", func(t *testing.T) {
		_, err := doUnpack(t, []byte{
			15<<3 | uint8(protoVARINT), 1, // dnstap.Type = Message
			14<<3 | uint8(protoLEN), 0, // dnstap.Message
		})
		mustErr(t, err)
	})

	t.Run("check Identity", func(t *testing.T) {
		dt, err := doUnpack(t, &dnstap.Dnstap{
			Identity: []byte("Test Identity"),
			Version:  []byte("Skipped"),
			Extra:    make([]byte, 1024*1024),
			Type:     new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type: new(dnstap.Message_AUTH_QUERY),
			},
		})
		noErr(t, err)
		if dt.Identity != "Test Identity" {
			t.Fatalf("dnstap.Identity not as expected")
		}
		checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType|WithIdentity)
	})

	t.Run("check replace", func(t *testing.T) {
		dt, err := doUnpack(t, &dnstap.Dnstap{
			Identity: []byte("Test Identity Original"),
			Type:     new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type:            new(dnstap.Message_AUTH_QUERY),
				SocketFamily:    new(dnstap.SocketFamily_INET),
				ResponseAddress: mpas("192.0.2.2"),
				ResponsePort:    new(uint32(53)),
				QueryMessage:    []byte("not this"),
				QueryTimeSec:    new(uint64(111111)),
				QueryTimeNsec:   new(uint32(222222)),
			},
		}, &dnstap.Dnstap{
			Identity: []byte("Test Identity Changed"),
			Type:     new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type:             new(dnstap.Message_AUTH_RESPONSE),
				SocketProtocol:   new(dnstap.SocketProtocol_UDP),
				QueryAddress:     mpas("192.0.2.1"),
				ResponseAddress:  mpas("192.0.2.3"),
				QueryMessage:     []byte("not this"),
				ResponseMessage:  []byte("test"),
				ResponseTimeSec:  new(uint64(333333)),
				ResponseTimeNsec: new(uint32(444444)),
			},
		})
		noErr(t, err)
		if dt.Identity != "Test Identity Changed" {
			t.Fatalf("dnstap.Identity not as expected")
		}
		if dt.SocketProtocol != 1 {
			t.Fatalf("dnstap.SocketProtocol not as expected")
		}
		if dt.QueryAddr != mpa("192.0.2.1") {
			t.Fatalf("dnstap.QueryAddr not as expected")
		}
		if dt.ResponseAddr != mpa("192.0.2.3") {
			t.Fatalf("dnstap.ResponseAddr not as expected")
		}
		if !slices.Equal(dt.Message, []byte("test")) {
			t.Fatalf("dnstap.Message not as expected")
		}
		if dt.Timestamp != time.Unix(333333, 444444).UTC() {
			t.Fatalf("dnstap.Timestamp not as expected")
		}
		checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType|WithDNSMessage|
			WithIdentity|
			ValidSocketProtocol|
			ValidQueryAddr|ValidResponseAddr|ValidResponsePort|
			hasQueryTime|hasResponseTime|ValidTime)
	})

	t.Run("valid, then invalid", func(t *testing.T) {
		valid := &dnstap.Dnstap{
			Type: new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type:             new(dnstap.Message_AUTH_RESPONSE),
				SocketFamily:     new(dnstap.SocketFamily_INET),
				QueryAddress:     mpas("192.0.2.54"),
				ResponseAddress:  mpas("192.0.2.53"),
				QueryPort:        new(uint32(54)),
				ResponsePort:     new(uint32(53)),
				ResponseTimeSec:  new(uint64(111111)),
				ResponseTimeNsec: new(uint32(222222)),
				ResponseMessage:  []byte{0},
			},
		}
		invalid := &dnstap.Dnstap{
			Type: new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type:            new(dnstap.Message_AUTH_RESPONSE),
				QueryAddress:    []byte{0},
				ResponseAddress: []byte{1},
				QueryPort:       new(uint32(math.MaxUint32)),
				ResponsePort:    new(uint32(math.MaxUint16) + 1),
				ResponseTimeSec: new(uint64(math.MaxInt64) + 1),
			},
		}
		dt, err := doUnpack(t, valid)
		noErr(t, err)
		checkFlags(t, dt, Valid|hasResponseTime)
		dt, err = doUnpack(t, valid, invalid)
		noErr(t, err)
		checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType|WithDNSMessage|hasResponseTime)
	})

	t.Run("tlv larger than one byte", func(t *testing.T) {
		dt, err := doUnpack(t, []byte{
			// test in dnstap
			0b10001010, 0b00000001, 0b00000100, 'T', 'E', 'S', 'T',
			// test in dnstap.Message
			14<<3 | uint8(protoLEN), 7, // dnstap.Message
			0b10001010, 0b00000001, 0b00000100, 'T', 'E', 'S', 'T',
		}, &dnstap.Dnstap{
			Type: new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type: new(dnstap.Message_AUTH_QUERY),
			},
		})
		noErr(t, err)
		checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType)
	})

	t.Run("skip unknown", func(t *testing.T) {
		dt, err := doUnpack(t, []byte{
			// test in dnstap
			0b00000000 | uint8(protoVARINT), 0b00000001, // VARINT
			0b00000000 | uint8(protoI64), 0, 0, 0, 0, 0, 0, 0, 0, // I64
			0b00000000 | uint8(protoLEN), 0b00000100, 'T', 'E', 'S', 'T', // LEN
			0b00000000 | uint8(protoI32), 0, 0, 0, 0, // I32
			// test in dnstap.Message
			14<<3 | uint8(protoLEN), 2 + 9 + 6 + 5, // dnstap.Message
			0b00000000 | uint8(protoVARINT), 0b00000001, // VARINT
			0b00000000 | uint8(protoI64), 0, 0, 0, 0, 0, 0, 0, 0, // I64
			0b00000000 | uint8(protoLEN), 0b00000100, 'T', 'E', 'S', 'T', // LEN
			0b00000000 | uint8(protoI32), 0, 0, 0, 0, // I32
		}, &dnstap.Dnstap{
			Type: new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type: new(dnstap.Message_AUTH_QUERY),
			},
		})
		noErr(t, err)
		checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType)
	})

	t.Run("check type for fields", func(t *testing.T) {
		t.Run("in dnstap", func(t *testing.T) {
			for _, i := range []uint8{1, 14, 15} {
				_, err := doUnpack(t, []byte{
					i<<3 | uint8(protoI64), 0, 0, 0, 0, 0, 0, 0, 0, // I64
				}, &dnstap.Dnstap{
					Type: new(dnstap.Dnstap_MESSAGE),
					Message: &dnstap.Message{
						Type: new(dnstap.Message_AUTH_QUERY),
					},
				})
				mustErr(t, err)
			}
		})
		t.Run("in dnstap.Message", func(t *testing.T) {
			for _, i := range []uint8{1, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14} {
				_, err := doUnpack(t, []byte{
					14<<3 | uint8(protoLEN), 9, // dnstap.Message
					i<<3 | uint8(protoI64), 0, 0, 0, 0, 0, 0, 0, 0, // I64
				}, &dnstap.Dnstap{
					Type: new(dnstap.Dnstap_MESSAGE),
					Message: &dnstap.Message{
						Type: new(dnstap.Message_AUTH_QUERY),
					},
				})
				mustErr(t, err)
			}
		})
	})

	t.Run("check timestamp", func(t *testing.T) {
		for _, qt := range []dnstap.Message_Type{dnstap.Message_AUTH_QUERY, dnstap.Message_AUTH_RESPONSE} {
			t.Run(dnstap.Message_Type_name[int32(qt)], func(t *testing.T) {
				// unset
				dt, err := doUnpack(t, &dnstap.Dnstap{
					Type: new(dnstap.Dnstap_MESSAGE),
					Message: &dnstap.Message{
						Type: new(qt),
					},
				})
				noErr(t, err)
				if !dt.Timestamp.Equal(time.Unix(0, 0).UTC()) {
					t.Fatal("Incorrect Timestamp")
				}
				checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType)
				// with seconds set
				dt, err = doUnpack(t, &dnstap.Dnstap{
					Type: new(dnstap.Dnstap_MESSAGE),
					Message: &dnstap.Message{
						Type:            new(qt),
						QueryTimeSec:    new(uint64(111111)),
						ResponseTimeSec: new(uint64(222222)),
					},
				})
				noErr(t, err)
				if dt.IsQuery && !dt.Timestamp.Equal(time.Unix(111111, 0).UTC()) {
					t.Fatal("Incorrect Timestamp")
				}
				if !dt.IsQuery && !dt.Timestamp.Equal(time.Unix(222222, 0).UTC()) {
					t.Fatal("Incorrect Timestamp")
				}
				checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType|ValidTime|hasQueryTime|hasResponseTime)
				// with nanoseconds set
				dt, err = doUnpack(t, &dnstap.Dnstap{
					Type: new(dnstap.Dnstap_MESSAGE),
					Message: &dnstap.Message{
						Type:             new(qt),
						QueryTimeNsec:    new(uint32(333333)),
						ResponseTimeNsec: new(uint32(444444)),
					},
				})
				noErr(t, err)
				if !dt.Timestamp.Equal(time.Unix(0, 0).UTC()) {
					t.Fatal("Incorrect Timestamp")
				}
				checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType)
				// with all set
				dt, err = doUnpack(t, &dnstap.Dnstap{
					Type: new(dnstap.Dnstap_MESSAGE),
					Message: &dnstap.Message{
						Type:             new(qt),
						QueryTimeSec:     new(uint64(111111)),
						ResponseTimeSec:  new(uint64(222222)),
						QueryTimeNsec:    new(uint32(333333)),
						ResponseTimeNsec: new(uint32(444444)),
					},
				})
				noErr(t, err)
				if dt.IsQuery && !dt.Timestamp.Equal(time.Unix(111111, 333333).UTC()) {
					t.Fatal("Incorrect Timestamp")
				}
				if !dt.IsQuery && !dt.Timestamp.Equal(time.Unix(222222, 444444).UTC()) {
					t.Fatal("Incorrect Timestamp")
				}
				checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType|ValidTime|hasQueryTime|hasResponseTime)
			})
		}
	})
}

func TestUnpackIPs(t *testing.T) {
	zero := netip.Addr{}
	tests := []struct {
		QueryAddr         []byte
		QueryAddrValid    bool
		ResponseAddr      []byte
		ResponseAddrValid bool
		SocketFamily      dnstap.SocketFamily
	}{
		// good
		{mpas("192.0.2.54"), true, mpas("192.0.2.53"), true, dnstap.SocketFamily_INET},
		{mpas("2001:db8::54"), true, mpas("2001:db8::53"), true, dnstap.SocketFamily_INET6},
		// completely wrong
		{mpas("192.0.2.54"), false, mpas("2001:db8::53"), false, dnstap.SocketFamily(0)},
		{mpas("192.0.2.54"), false, mpas("192.0.2.53"), false, dnstap.SocketFamily_INET6},
		{mpas("2001:db8::54"), false, mpas("2001:db8::53"), false, dnstap.SocketFamily_INET},
		// half wrong
		{mpas("192.0.2.54"), true, mpas("2001:db8::53"), false, dnstap.SocketFamily_INET},
		{mpas("2001:db8::54"), true, mpas("192.0.2.53"), false, dnstap.SocketFamily_INET6},
		{mpas("2001:db8::54"), false, mpas("192.0.2.53"), true, dnstap.SocketFamily_INET},
		{mpas("192.0.2.54"), false, mpas("2001:db8::53"), true, dnstap.SocketFamily_INET6},
		// length error
		{[]byte{1, 2, 3}, false, []byte{1, 2, 3, 4}, true, dnstap.SocketFamily_INET},
		{[]byte{1, 2, 3}, false, []byte{1, 2, 3, 4}, false, dnstap.SocketFamily_INET6},
		{nil, false, []byte{}, false, dnstap.SocketFamily_INET6},
	}
	for _, tc := range tests {
		// build dnstap message
		msg := &dnstap.Dnstap{
			Type: new(dnstap.Dnstap_MESSAGE),
			Message: &dnstap.Message{
				Type:            new(dnstap.Message_AUTH_RESPONSE),
				SocketFamily:    new(tc.SocketFamily),
				QueryAddress:    tc.QueryAddr,
				ResponseAddress: tc.ResponseAddr,
			},
		}
		// unpack
		dt, err := doUnpack(t, msg)
		noErr(t, err)
		// build and check flags
		flags := WithDnstapType | WithMessage | WithMessageType
		if tc.QueryAddrValid {
			flags |= ValidQueryAddr
		}
		if tc.ResponseAddrValid {
			flags |= ValidResponseAddr
		}
		checkFlags(t, dt, flags)
		// test address fields
		if tc.QueryAddrValid {
			if !bytes.Equal(tc.QueryAddr, dt.QueryAddr.AsSlice()) {
				t.Fatal("Incorrect query address")
			}
		} else {
			if dt.QueryAddr != zero {
				t.Fatal("Query address set when it shouldn't")
			}
		}
		if tc.ResponseAddrValid {
			if !bytes.Equal(tc.ResponseAddr, dt.ResponseAddr.AsSlice()) {
				t.Fatal("Incorrect response address")
			}
		} else {
			if dt.ResponseAddr != zero {
				t.Fatal("Response address set when it shouldn't")
			}
		}
	}
}

func TestOverflows(t *testing.T) {
	// build dnstap message
	msg := &dnstap.Dnstap{
		Type: new(dnstap.Dnstap_MESSAGE),
		Message: &dnstap.Message{
			QueryPort:       new(uint32(math.MaxUint32)),
			ResponsePort:    new(uint32(math.MaxUint32)),
			QueryTimeSec:    new(uint64(math.MaxUint64)),
			ResponseTimeSec: new(uint64(math.MaxUint64)),
			SocketProtocol:  new(dnstap.SocketProtocol(math.MaxInt32)),
		},
	}
	// test a response
	msg.Message.Type = new(dnstap.Message_AUTH_RESPONSE)
	dt, err := doUnpack(t, msg)
	noErr(t, err)
	checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType|hasQueryTime|hasResponseTime)
	if dt.QueryPort != 0 {
		t.Fatal("Incorrect query port")
	}
	if dt.ResponsePort != 0 {
		t.Fatal("Incorrect response port")
	}
	if dt.SocketProtocol != 0 {
		t.Fatal("Incorrect socket protocol")
	}
	if !dt.Timestamp.Equal(time.Unix(0, 0).UTC()) {
		t.Fatal("Incorrect response time")
	}
	// test timestamp for query
	msg.Message.Type = new(dnstap.Message_AUTH_QUERY)
	dt, err = doUnpack(t, msg)
	noErr(t, err)
	checkFlags(t, dt, WithDnstapType|WithMessage|WithMessageType|hasQueryTime|hasResponseTime)
	if !dt.Timestamp.Equal(time.Unix(0, 0).UTC()) {
		t.Fatal("Incorrect query time")
	}
}

func checkFlags(t *testing.T, dt *Message, expected Flag) {
	t.Helper()
	if dt.flags != expected {
		t.Fatalf("Incorrect flags: expected %b actually %b", expected, dt.flags)
	}
}

func doUnpack(t *testing.T, payloads ...any) (*Message, error) {
	t.Helper()
	buf := []byte{}
	for _, _payload := range payloads {
		switch payload := _payload.(type) {
		case *dnstap.Dnstap:
			pb, err := proto.Marshal(payload)
			if err != nil {
				t.Fatalf("Could not marshal Dnstap message: %v", err)
			}
			buf = append(buf, pb...)
		case []byte:
			buf = append(buf, payload...)
		default:
			panic("unknown type")
		}
	}
	dt := Message{}
	return &dt, dt.Unpack(buf)
}

func TestReadVARINT(t *testing.T) {
	// check 0...127
	for i := range uint8(128) {
		checkReadVARINT(t, []byte{i}, uint64(i))
	}
	// check 128...255
	for i := range uint8(128) {
		checkReadVARINT(t, []byte{0b10000000 + i, 0b00000001}, uint64(128+i))
	}
	// a random number
	checkReadVARINT(t, []byte{0xc0, 0xb4, 0xfd, 0x98, 0xbe, 0xe6, 0xc4, 0xdc, 0xb9, 0x01}, 13382748881282882112)
	// check maximum value
	checkReadVARINT(t, []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 127}, math.MaxUint64)
	// test length 11
	panics(t, "VARINT overflow", func() { readVARINT([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 127}) })
}

func checkReadVARINT(t *testing.T, in []byte, expected uint64) {
	t.Helper()
	actually, out := readVARINT(in)
	if !slices.Equal(skipVARINT(in), out) {
		t.Fatal("Skipped incorrectly: reading differently than skipVARINT")
	}
	if expected != actually {
		t.Fatalf("Wrong number: expected %v actually %v", expected, actually)
	}
}

func TestSkipVARINT(t *testing.T) {
	// test length 1
	in := []byte{0b00000000}
	checkSkipVARINT(t, in, 1)
	// test length 2...10
	for i := range 9 {
		in = append([]byte{0b10000000}, in...)
		checkSkipVARINT(t, in, 2+i)
	}
	// test length 11
	panics(t, "VARINT overflow", func() { skipVARINT([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 127}) })
}

func checkSkipVARINT(t *testing.T, in []byte, offset int) {
	t.Helper()
	out := skipVARINT(in)
	if !slices.Equal(in[offset:], out) {
		t.Fatalf("Skipped incorrectly: expected %v actually %v", in[offset:], out)
	}
}

func panics(t *testing.T, expected string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("test did not panic")
		}
		if fmt.Sprintf("%v", r) != expected {
			t.Fatalf("incorrect panic, expected '%v' actually '%v'", expected, r)
		}
	}()
	fn()
}
