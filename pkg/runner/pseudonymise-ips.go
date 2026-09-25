package runner

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"

	"github.com/dnstapir/edm/pkg/dnstap"
)

// update the pseudonymiser with a new key
func (edm *DnstapMinimiser) setPseudonymiseKey(keystring string, salt string) error {
	// generate a key based on the supplied keystring and salt, we use a KDF simply
	// because we have a hard time knowing the quality and don't control the length
	// of the supplied keystring. We are using pbkdf2 partly because it exist in go's
	// stdlib, and because it was been deamed suitable together with aes128 for the
	// usecase at hand.
	key, err := pbkdf2.Key(sha1.New, keystring, []byte("edm:"+salt), 4096, 16)
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

func (edm *DnstapMinimiser) getPseudonymiser() cipher.Block {
	pseudonymiser := edm.pseudonymiser.Load()
	// if a pseudonymiser isn't set
	if pseudonymiser == nil {
		// use one that zeros out the input
		return zeroCipher{}
	}
	return *pseudonymiser
}

func (edm *DnstapMinimiser) pseudonymiseIPs(dt *dnstap.Message) {
	pseudonymiser := edm.getPseudonymiser()
	dt.QueryAddr = pseudonymiseIP(pseudonymiser, dt.QueryAddr)
	dt.ResponseAddr = pseudonymiseIP(pseudonymiser, dt.ResponseAddr)
}

// Pseudonymise the IP using a block cipher. The block cipher is
// essentially used as a "hash function" but with a guaranteed and
// secret (guarded by the key) 1-1 mapping.
// Note: the supplied cipher must have a block size of 16 bytes.
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

// To be used on an pseudonymised IP address to extract an identifier that
// fits inside an uint64. It currently discards half of the address bytes.
func ipToIdentifier(ip netip.Addr) uint64 {
	buf := ip.As16()
	return binary.BigEndian.Uint64(buf[:])
}

// zeroCipher is a implementation of cipher.Block with the block size of 16
// that always sets dst to zero regardless of src.
type zeroCipher struct{}

func (z zeroCipher) BlockSize() int                 { return 16 }
func (z zeroCipher) Encrypt(dst []byte, src []byte) { z.zero(dst) }
func (z zeroCipher) Decrypt(dst []byte, src []byte) { z.zero(dst) }

func (zeroCipher) zero(dst []byte) {
	for i := range 16 {
		dst[i] = 0
	}
}
