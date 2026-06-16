package runner

import (
	"crypto/x509"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lestrrat-go/jwx/v3/jwa"
)

func TestDefaultDependenciesFillCryptopanFactory(t *testing.T) {
	deps := fillDependencies(dependencies{})
	if deps.CryptopanFactory == nil {
		t.Fatal("CryptopanFactory was not filled")
	}
	if reflect.TypeOf(deps.CryptopanFactory) != reflect.TypeOf(realCryptopanFactory{}) {
		t.Fatalf("CryptopanFactory type = %T, want realCryptopanFactory", deps.CryptopanFactory)
	}
}

func TestCertPoolAndJWKFiles(t *testing.T) {
	loader := realKeyMaterialLoader{fs: osFileSystem{}}

	certPath, tlsKeyPath, caPath := testCertFiles(t)
	pool, err := loader.LoadCertPool(caPath)
	if err != nil {
		t.Fatal(err)
	}
	if pool.Equal(x509.NewCertPool()) {
		t.Fatal("cert pool has no certificates")
	}

	cert, err := loader.LoadKeyPair(certPath, tlsKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("TLS key pair has no certificates")
	}
	if _, err := loader.LoadKeyPair(certPath, filepath.Join(t.TempDir(), "missing-key.pem")); err == nil {
		t.Fatal("missing TLS key file succeeded")
	}
	faultingLoader := realKeyMaterialLoader{
		fs: faultingFileSystem{
			fileSystem: osFileSystem{},
			readFile: func(string) ([]byte, error) {
				return nil, errInjected
			},
		},
	}
	if _, err := faultingLoader.LoadKeyPair(certPath, tlsKeyPath); !errors.Is(err, errInjected) {
		t.Fatalf("faulting LoadKeyPair error = %v, want errInjected", err)
	}

	if _, err := loader.LoadCertPool(writeTempFile(t, "bad-ca.pem", []byte("not pem"))); err == nil {
		t.Fatal("bad CA file succeeded")
	}
	if _, err := loader.LoadCertPool(filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Fatal("missing CA file succeeded")
	}

	jwkPath := testJWKFile(t)
	key, err := loader.LoadEdDSAJWK(jwkPath)
	if err != nil {
		t.Fatal(err)
	}
	alg, ok := key.Algorithm()
	if !ok || alg != jwa.EdDSA() {
		t.Fatalf("algorithm = %v", alg)
	}
	if _, err := loader.LoadEdDSAJWK(writeTempFile(t, "bad.jwk", []byte("{"))); err == nil {
		t.Fatal("bad JWK succeeded")
	}
	if _, err := loader.LoadEdDSAJWK(filepath.Join(t.TempDir(), "missing.jwk")); err == nil {
		t.Fatal("missing JWK succeeded")
	}
	// An EC key is not an OKP key at all (example key from RFC 7515 A.3).
	ecJWK := `{"kty":"EC","crv":"P-256","x":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU","y":"x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0","d":"jpsQnnGQmL-YBIffH1136cspYG6-0iY7X1fCE9-E9LI"}`
	if _, err := loader.LoadEdDSAJWK(writeTempFile(t, "ec.jwk", []byte(ecJWK))); !errors.Is(err, errNotEdDSAJWK) {
		t.Fatalf("EC JWK error = %v, want errNotEdDSAJWK", err)
	}
	// X25519 is an OKP key-agreement curve, not an EdDSA signing curve
	// (example key from RFC 8037 A.6).
	x25519JWK := `{"kty":"OKP","crv":"X25519","x":"hSDwCYkwp1R0i33ctD73Wg2_Og0mOBr066SpjqqbTmo"}`
	if _, err := loader.LoadEdDSAJWK(writeTempFile(t, "x25519.jwk", []byte(x25519JWK))); !errors.Is(err, errNotEdDSAJWK) {
		t.Fatalf("X25519 JWK error = %v, want errNotEdDSAJWK", err)
	}
	// A valid Ed25519 signing key but with no key ID (RFC 8037 A.1).
	noKidJWK := `{"kty":"OKP","crv":"Ed25519","d":"nWGxne_9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2A","x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}`
	if _, err := loader.LoadEdDSAJWK(writeTempFile(t, "nokid.jwk", []byte(noKidJWK))); !errors.Is(err, errJWKMissingKeyID) {
		t.Fatalf("kid-less JWK error = %v, want errJWKMissingKeyID", err)
	}
}

func TestLoadDawgFileErrors(t *testing.T) {
	loader := realDawgLoader{fs: osFileSystem{}}

	if _, _, err := loader.LoadDawgFile(filepath.Join(t.TempDir(), "missing.dawg")); err == nil {
		t.Fatal("missing DAWG succeeded")
	}
	if _, _, err := loader.LoadDawgFile(writeTempFile(t, "empty.dawg", nil)); !errors.Is(err, errEmptyDawgFile) {
		t.Fatalf("empty DAWG error = %v", err)
	}
	recovered := func() (recovered any) {
		defer func() {
			recovered = recover()
		}()
		if _, _, err := loader.LoadDawgFile(writeTempFile(t, "invalid.dawg", []byte("bad"))); err != nil {
			t.Fatalf("invalid DAWG returned error instead of panic: %s", err)
		}
		return nil
	}()
	if recovered == nil {
		t.Fatal("invalid DAWG did not panic")
	}

	finder, _, err := loader.LoadDawgFile(testDawgFile(t, "example.com."))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := finder.Close(); err != nil {
			t.Fatalf("close loaded DAWG: %s", err)
		}
	})
	if finder.NumAdded() != 1 {
		t.Fatalf("NumAdded = %d", finder.NumAdded())
	}
}
