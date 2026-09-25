package runner

import (
	"net/netip"
	"testing"

	extdnstap "github.com/dnstap/golang-dnstap"
)

var mpa = netip.MustParseAddr

func TestPseudonymiser(t *testing.T) {
	edm := DnstapMinimiser{}

	// make sure that the pseudonymiser is unset
	if edm.pseudonymiser.Load() != nil {
		t.Fatal("pseudonymiser return a zeroCipher")
	}
	// without a specified cipher our addresses and identifiers will be set to
	// all zeros, but it will still correctly determine IdentifierTypes
	testPseudonymiser(t, &edm, pseudonymiserTestCase{
		mpa("192.0.2.1").AsSlice(), mpa("192.0.2.2").AsSlice(), false,
		mpa("::"), mpa("::"),
		0, 0,
		IdentifierIPv4, IdentifierIPv4,
	})

	// setup a cipher
	if err := edm.setPseudonymiseKey("key", "salt"); err != nil {
		t.Fatalf("Got error setting pseudonymiser's key: %v", err)
	}
	// make sure that the pseudonymiser is now set
	if edm.pseudonymiser.Load() == nil {
		t.Fatal("pseudonymiser should not be nil")
	}

	// and this will give us pseudonymised addresses and identifiers and
	// correctly determined IdentifierTypes
	testPseudonymiser(t, &edm, pseudonymiserTestCase{
		mpa("2001:db8::1").AsSlice(), mpa("192.0.2.2").AsSlice(), false,
		mpa("8861:2cd7:a506:c761:b7a4:7ff8:2f38:3fbf"), mpa("e3f0:2ac6:b5a5:5b3b:901b:87a0:ecc3:457c"),
		0x88612cd7a506c761, 0xe3f02ac6b5a55b3b,
		IdentifierOther, IdentifierIPv4,
	})

	// changing the socket family does not change the identifiers, only the
	// determined IdentifierTypes
	testPseudonymiser(t, &edm, pseudonymiserTestCase{
		mpa("2001:db8::1").AsSlice(), mpa("192.0.2.2").AsSlice(), true,
		mpa("8861:2cd7:a506:c761:b7a4:7ff8:2f38:3fbf"), mpa("e3f0:2ac6:b5a5:5b3b:901b:87a0:ecc3:457c"),
		0x88612cd7a506c761, 0xe3f02ac6b5a55b3b,
		IdentifierIPv6, IdentifierOther,
	})

	// swapping the key
	if err := edm.setPseudonymiseKey("KEY", "salt"); err != nil {
		t.Fatalf("Got error setting pseudonymiser's key: %v", err)
	}

	// will however change the pseudonymised addresses and identifiers, but
	// maintain the determined IdentifierTypes
	testPseudonymiser(t, &edm, pseudonymiserTestCase{
		mpa("2001:db8::1").AsSlice(), mpa("192.0.2.2").AsSlice(), true,
		mpa("427c:ff47:ae1f:1be1:fece:6c90:ba03:ab22"), mpa("975e:e305:7632:26f8:a1a9:1515:2454:200d"),
		0x427cff47ae1f1be1, 0x975ee305763226f8,
		IdentifierIPv6, IdentifierOther,
	})

	// broken addresses do also generate a pseudonymised address and identifier
	testPseudonymiser(t, &edm, pseudonymiserTestCase{
		nil,
		[]byte{1},
		true,
		mpa("5175:7a64:4d90:ad63:3664:5188:ec00:d575"), mpa("889b:2362:2c3f:1217:915c:8ce:aef5:4526"),
		0x51757a644d90ad63, 0x889b23622c3f1217,
		IdentifierOther, IdentifierOther,
	})
	// some "addresses" overlap
	testPseudonymiser(t, &edm, pseudonymiserTestCase{
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		true,
		mpa("fc73:8a7f:e936:6523:301c:35c8:ac:c21a"), mpa("fc73:8a7f:e936:6523:301c:35c8:ac:c21a"),
		0xfc738a7fe9366523, 0xfc738a7fe9366523,
		IdentifierIPv6, IdentifierOther,
	})
	testPseudonymiser(t, &edm, pseudonymiserTestCase{
		[]byte{1, 0, 0, 0, 0},
		[]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		true,
		mpa("889b:2362:2c3f:1217:915c:8ce:aef5:4526"), mpa("889b:2362:2c3f:1217:915c:8ce:aef5:4526"),
		0x889b23622c3f1217, 0x889b23622c3f1217,
		IdentifierOther, IdentifierIPv6,
	})
}

type pseudonymiserTestCase struct {
	// input
	InQueryAddr    []byte
	InResponseAddr []byte
	InIsIPv6       bool
	// expected output
	ExpQueryAddr              netip.Addr
	ExpResponseAddr           netip.Addr
	ExpQueryIdentifier        uint64
	ExpResponseIdentifier     uint64
	ExpQueryIdentifierType    IdentifierType
	ExpResponseIdentifierType IdentifierType
}

func testPseudonymiser(t *testing.T, edm *DnstapMinimiser, tc pseudonymiserTestCase) {
	t.Helper()

	// create a dnstap message
	dt := testUnpackedMinimalDnstapMessage(t, tc.InIsIPv6, func(dt *extdnstap.Dnstap) {
		dt.Message.QueryAddress = tc.InQueryAddr
		dt.Message.ResponseAddress = tc.InResponseAddr
	})

	// apply the pseudonymiser
	pdt := edm.pseudonymiseIPs(dt)

	// check the results
	if dt != pdt.Message {
		t.Fatal("pseudonymised does not contain the same supplied dnstap.Message")
	}
	if pdt.QueryAddr != dt.QueryAddr || pdt.ResponseAddr != dt.ResponseAddr {
		t.Fatal("pseudonymised.*Addr does not correspond to dnstap.Message.*Addr")
	}

	if dt.QueryAddr != tc.ExpQueryAddr {
		t.Fatalf("QueryAddr is not pseudonymised correctly, got: %v expected: %v",
			dt.QueryAddr, tc.ExpQueryAddr)
	}
	if dt.ResponseAddr != tc.ExpResponseAddr {
		t.Fatalf("ResponseAddr is not pseudonymised correctly, got: %v expected: %v",
			dt.ResponseAddr, tc.ExpResponseAddr)
	}

	if pdt.QueryAddrAsIdentifier() != tc.ExpQueryIdentifier {
		t.Fatalf("QueryAddr's Identifier were not correct, got: 0x%x expected: 0x%x",
			pdt.QueryAddrAsIdentifier(), tc.ExpQueryIdentifier)
	}
	if pdt.ResponseAddrAsIdentifier() != tc.ExpResponseIdentifier {
		t.Fatalf("ResponseAddr's Identifier were not correct, got: 0x%x expected: 0x%x",
			pdt.ResponseAddrAsIdentifier(), tc.ExpResponseIdentifier)
	}

	if pdt.QueryAddrType != tc.ExpQueryIdentifierType {
		t.Fatalf("QueryAddr's IdentifierType were not correct, got: %v expected: %v",
			pdt.QueryAddrType, tc.ExpQueryIdentifierType)
	}
	if pdt.ResponseAddrType != tc.ExpResponseIdentifierType {
		t.Fatalf("ResponseAddr's IdentifierType were not correct, got: %v expected: %v",
			pdt.ResponseAddrType, tc.ExpResponseIdentifierType)
	}
}

func TestAddressToIdentifierType(t *testing.T) {
	cases := []struct {
		addr     netip.Addr
		valid    bool
		expected IdentifierType
	}{
		{mpa("192.0.2.0"), true, IdentifierIPv4},
		{mpa("2001:db8::"), true, IdentifierIPv6},
		{mpa("192.0.2.0"), false, IdentifierOther},
		{mpa("2001:db8::"), false, IdentifierOther},
		{netip.Addr{}, false, IdentifierOther},
		// should not happen, but is still handled gracefully
		{netip.Addr{}, true, IdentifierOther},
	}

	for _, tc := range cases {
		got := addressToIdentifierType(tc.addr, tc.valid)
		if got != tc.expected {
			t.Fatalf("Expected %s to be a IdentifierType(%d)", tc.addr, tc.expected)
		}
	}
}
