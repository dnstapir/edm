package runner

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"

	"github.com/dnstapir/edm/pkg/dnstap"
)

// update the pseudonymiser with a new key
func (edm *DnstapMinimiser) setPseudonymiseKey(keystring string, salt string) error {
	// generate a key based on the supplied keystring and salt, we use a KDF
	// simply because we have a hard time knowing the quality and don't control
	// the length of the supplied keystring. We are using pbkdf2 partly because
	// it exist in go's stdlib, and because it was deamed suitable together with
	// aes128 for the usecase at hand.
	key, err := pbkdf2.Key(sha256.New, keystring, []byte("edm:"+salt), 4096, 16)
	if err != nil {
		return fmt.Errorf("setPseudonymiseKey: %w", err)
	}
	// create the new cipher and verify it
	cipher, err := aes.NewCipher(key)
	if err != nil {
		return fmt.Errorf("setPseudonymiseKey: %w", err)
	}
	if cipher.BlockSize() != 16 {
		return errors.New("setPseudonymiseKey: cipher does not have block size 16")
	}
	// update the pseudonymiser
	edm.pseudonymiser.Store(&cipher)
	return nil
}

type pseudonymised struct {
	*dnstap.Message
	QueryAddrType    IdentifierType
	ResponseAddrType IdentifierType
}

func (pdt *pseudonymised) QueryAddrAsIdentifier() uint64    { return ipToIdentifier(pdt.QueryAddr) }
func (pdt *pseudonymised) ResponseAddrAsIdentifier() uint64 { return ipToIdentifier(pdt.ResponseAddr) }

func (edm *DnstapMinimiser) pseudonymiseIPs(dt *dnstap.Message) pseudonymised {
	// create the return value
	pdt := pseudonymised{
		Message:          dt,
		QueryAddrType:    addressToIdentifierType(dt.QueryAddr, dt.HasFlags(dnstap.ValidQueryAddr)),
		ResponseAddrType: addressToIdentifierType(dt.ResponseAddr, dt.HasFlags(dnstap.ValidResponseAddr)),
	}
	// extract and use pseudonymiser
	pseudonymiser := edm.pseudonymiser.Load()
	if pseudonymiser == nil {
		// if no cipher is supplied => default to set addresses to the "::" IPv6 address
		dt.QueryAddr = netip.IPv6Unspecified()
		dt.ResponseAddr = netip.IPv6Unspecified()
	} else {
		// else pseudonymise the query and response addresses using the supplied cipher
		dt.QueryAddr = pseudonymiseIP(*pseudonymiser, dt.QueryAddr)
		dt.ResponseAddr = pseudonymiseIP(*pseudonymiser, dt.ResponseAddr)
	}
	// return the pseudonymised struct
	return pdt
}

// Helper to pseudonymise the IP addresss using a block cipher. The block
// cipher is essentially used as a "hash function" but with a guaranteed and
// secret (guarded by the key) 1-1 mapping.
// NOTE: the supplied cipher must have a block size of 16 bytes.
// NOTE: only to be used by edm.pseudonymiseIPs
func pseudonymiseIP(cipher cipher.Block, ip netip.Addr) netip.Addr {
	// extract IP as 16-bytes, IPv6 is mapped directly,
	// IPv4 is mapped under ::ffff:/96, the zero value
	// is mapped to all zeros
	buf := ip.As16()
	// encrypt the bytes
	cipher.Encrypt(buf[:], buf[:])
	// return the bytes to the netip.Addr format
	return netip.AddrFrom16(buf)
}

// Used to specify the origin of the identifier
type IdentifierType uint8

const (
	IdentifierOther IdentifierType = 0 // unknown source of identifier
	IdentifierIPv4  IdentifierType = 4 // identifier is based on an IPv4 address
	IdentifierIPv6  IdentifierType = 6 // identifier is based on an IPv6 address
)

// Helper used to determine the source of an IP address
// NOTE: only to be used by edm.pseudonymiseIPs
func addressToIdentifierType(addr netip.Addr, valid bool) IdentifierType {
	switch {
	case valid && addr.Is4():
		return IdentifierIPv4
	case valid && addr.Is6():
		return IdentifierIPv6
	default:
		return IdentifierOther
	}
}

// Helper used on a pseudonymised IP address to extract an identifier that fits
// inside an uint64. It currently discards half of the address bytes.
// NOTE: only to be used by pseudonymised's methods
func ipToIdentifier(ip netip.Addr) uint64 {
	buf := ip.As16()
	return binary.BigEndian.Uint64(buf[:])
}
