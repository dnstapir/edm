package runner

import (
	"errors"
	"testing"
)

func TestCertStore(t *testing.T) {
	store := newCertStore()
	if _, err := store.getClientCertificate(nil); !errors.Is(err, errNoClientCertificate) {
		t.Fatalf("empty getClientCertificate error = %v", err)
	}

	certPath, keyPath, _ := testCertFiles(t)
	if err := store.loadCert(realKeyMaterialLoader{fs: osFileSystem{}}, certPath, keyPath); err != nil {
		t.Fatal(err)
	}
	cert, err := store.getClientCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("loaded certificate is empty")
	}

	if err := store.loadCert(realKeyMaterialLoader{fs: osFileSystem{}}, certPath, keyPath+".missing"); err == nil {
		t.Fatal("loadCert with missing key succeeded")
	}
}
