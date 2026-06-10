package runner

import (
	"crypto/x509"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lestrrat-go/jwx/v2/jwa"
)

func TestDefaultDependenciesFillCryptopanFactory(t *testing.T) {
	deps := fillDependencies(Dependencies{})
	if deps.CryptopanFactory == nil {
		t.Fatal("CryptopanFactory was not filled")
	}
	if reflect.TypeOf(deps.CryptopanFactory) != reflect.TypeOf(realCryptopanFactory{}) {
		t.Fatalf("CryptopanFactory type = %T, want realCryptopanFactory", deps.CryptopanFactory)
	}
}

func TestCertPoolAndJWKFiles(t *testing.T) {
	loader := realKeyMaterialLoader{fs: osFileSystem{}}

	_, _, caPath := testCertFiles(t)
	pool, err := loader.LoadCertPool(caPath)
	if err != nil {
		t.Fatal(err)
	}
	if pool.Equal(x509.NewCertPool()) {
		t.Fatal("cert pool has no certificates")
	}

	if _, err := loader.LoadCertPool(writeTempFile(t, "bad-ca.pem", []byte("not pem"))); err == nil {
		t.Fatal("bad CA file succeeded")
	}
	if _, err := loader.LoadCertPool(filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Fatal("missing CA file succeeded")
	}

	keyPath := testJWKFile(t)
	key, err := loader.LoadEdDSAJWK(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if key.Algorithm() != jwa.EdDSA {
		t.Fatalf("algorithm = %v", key.Algorithm())
	}
	if _, err := loader.LoadEdDSAJWK(writeTempFile(t, "bad.jwk", []byte("{"))); err == nil {
		t.Fatal("bad JWK succeeded")
	}
	if _, err := loader.LoadEdDSAJWK(filepath.Join(t.TempDir(), "missing.jwk")); err == nil {
		t.Fatal("missing JWK succeeded")
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
