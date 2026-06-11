package runner

import (
	"errors"
	"fmt"
	"net/netip"

	dnstap "github.com/dnstap/golang-dnstap"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/yawning/cryptopan"
	"golang.org/x/crypto/argon2"
)

// Create a 32 byte length secret based on the supplied -crypto-pan key,
// this way the user can supply a -cryptopan-key of any length and
// we still end up with the 32 byte length expected by AES.
//
// Using a proper password KDF (argon2) might be overkill as we are not
// storing the resulting hash anywhere, but it only affects startup/key
// rotation time of a mostly long running tool.
func getCryptopanAESKey(key string, salt string) []byte {
	var aesKeyLen uint32 = 32
	aesKey := argon2.IDKey([]byte(key), []byte(salt), 1, 64*1024, 4, aesKeyLen)
	return aesKey
}

type realCryptopanFactory struct{}

func (realCryptopanFactory) NewCryptopan(key, salt string) (*cryptopan.Cryptopan, error) {
	return createCryptopan(key, salt)
}

func (edm *DnstapMinimiser) setCryptopan(key string, salt string, cacheEntries int) error {
	// cacheEntries is the per-worker LRU size, validated here so a bad config
	// value surfaces at load time instead of crashing a worker when it builds
	// its LRU. The caches themselves are owned by each minimiser worker (see
	// runMinimiser) and are sized once at worker start; setCryptopan only
	// installs the cryptopan instance and signals workers to purge their caches
	// on the next call. Changing cryptopan-address-entries therefore takes
	// effect only on restart, which is why that field is not reloadable.
	if cacheEntries < 0 {
		return fmt.Errorf("setCryptopan: invalid cache size %d", cacheEntries)
	}

	cpn, err := edm.deps.CryptopanFactory.NewCryptopan(key, salt)
	if err != nil {
		return fmt.Errorf("setCryptopan: unable to create cryptopan: %w", err)
	}

	edm.cryptopan.Store(cpn)
	edm.cryptopanGen.Add(1)

	return nil
}

func createCryptopan(key string, salt string) (*cryptopan.Cryptopan, error) {
	cryptopanKey := getCryptopanAESKey(key, salt)

	cpn, err := cryptopan.New(cryptopanKey)
	if err != nil {
		return nil, fmt.Errorf("createCryptopan: %w", err)
	}

	return cpn, nil
}

// pseudonymiseDnstap pseudonymises the IP address fields in a dnstap message.
//
// cache is the caller's per-worker LRU; cpn is a snapshot of the current
// cryptopan instance taken once per frame so QueryAddress and ResponseAddress
// see the same key. The per-worker cache and cryptopan snapshot are passed in
// rather than read from shared state, so the hot path needs no locking.
func (edm *DnstapMinimiser) pseudonymiseDnstap(dt *dnstap.Dnstap, cpn *cryptopan.Cryptopan, cache *lru.Cache[netip.Addr, netip.Addr]) {
	var err error
	if dt.Message.QueryAddress != nil {
		dt.Message.QueryAddress, err = edm.pseudonymiseIP(dt.Message.QueryAddress, cpn, cache)
		if err != nil {
			edm.log.Error("pseudonymiseDnstap: unable to parse dt.Message.QueryAddress", "error", err)
		}
	}
	if dt.Message.ResponseAddress != nil {
		dt.Message.ResponseAddress, err = edm.pseudonymiseIP(dt.Message.ResponseAddress, cpn, cache)
		if err != nil {
			edm.log.Error("pseudonymiseDnstap: unable to parse dt.Message.ResponseAddress", "error", err)
		}
	}
}

// Pseudonymise IP address, even on error the returned []byte is usable (zeroed address).
// Caller passes the per-worker cache and the cryptopan snapshot; nil cache disables caching.
func (edm *DnstapMinimiser) pseudonymiseIP(ipBytes []byte, cpn *cryptopan.Cryptopan, cache *lru.Cache[netip.Addr, netip.Addr]) ([]byte, error) {
	addr, ok := netip.AddrFromSlice(ipBytes)
	if !ok {
		// Replace address with zeroes since we do not know if
		// the contained junk is somehow sensitive
		return make([]byte, len(ipBytes)), errors.New("unable to parse addr")
	}

	var pseudonymisedAddr netip.Addr
	var cacheHit bool

	if cache != nil {
		pseudonymisedAddr, cacheHit = cache.Get(addr)
	}

	if cacheHit {
		edm.promCryptopanCacheHit.Inc()
	} else {
		// Not in cache or cache disabled, calculate the pseudonymised IP
		pseudonymisedAddr, ok = netip.AddrFromSlice(cpn.Anonymize(addr.AsSlice()))
		if !ok {
			// Replace address with zeroes here as well
			// since we do not know if the contained junk
			// is somehow sensitive.
			return make([]byte, len(ipBytes)), errors.New("unable to anonymise addr")
		}

		// cryptopan.Anonymize() returns IPv4 addresses via net.IPv4(),
		// meaning we will get IPv4 addresses mapped to IPv6, e.g.
		// ::ffff:127.0.0.1. It is easier to handle these as native
		// IPv4 addresses in our system so call Unmap() on it.
		pseudonymisedAddr = pseudonymisedAddr.Unmap()

		if cache != nil {
			evicted := cache.Add(addr, pseudonymisedAddr)
			if evicted {
				edm.promCryptopanCacheEvicted.Inc()
			}
		}
	}

	return pseudonymisedAddr.AsSlice(), nil
}
