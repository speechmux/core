package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeTempCertFiles generates a self-signed ECDSA certificate and writes the
// PEM-encoded cert and key to temporary files. It returns the file paths and
// registers cleanup via t.Cleanup.
func writeTempCertFiles(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "speechmux-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	dir := t.TempDir()

	certPath = filepath.Join(dir, "cert.pem")
	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatalf("encode cert: %v", err)
	}
	certFile.Close()

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPath = filepath.Join(dir, "key.pem")
	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	if err := pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}); err != nil {
		t.Fatalf("encode key: %v", err)
	}
	keyFile.Close()

	return certPath, keyPath
}

func TestBuildTLSConfig_Disabled(t *testing.T) {
	// When both CertFile and KeyFile are nil, TLS is disabled.
	cfg := TLSConfig{}
	got, err := BuildTLSConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil *tls.Config when cert/key not set")
	}
}

func TestBuildTLSConfig_MissingFile(t *testing.T) {
	cert := "/nonexistent/cert.pem"
	key := "/nonexistent/key.pem"
	cfg := TLSConfig{CertFile: &cert, KeyFile: &key}

	_, err := BuildTLSConfig(cfg)
	if err == nil {
		t.Fatal("expected error for missing cert/key files")
	}
}

func TestBuildTLSConfig_ValidCert(t *testing.T) {
	certPath, keyPath := writeTempCertFiles(t)
	cfg := TLSConfig{CertFile: &certPath, KeyFile: &keyPath}

	got, err := BuildTLSConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil *tls.Config")
	}
	if len(got.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(got.Certificates))
	}
	if got.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected MinVersion TLS 1.2, got %d", got.MinVersion)
	}
}

func TestValidate_TLSRequired_MissingCert(t *testing.T) {
	cfg := &Config{}
	cfg.Defaults()
	cfg.TLS.Required = true
	// CertFile and KeyFile remain nil.

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when tls_required=true but cert/key not set")
	}
}

func TestValidate_TLSRequired_WithCert(t *testing.T) {
	certPath, keyPath := writeTempCertFiles(t)
	cfg := &Config{}
	cfg.Defaults()
	cfg.TLS.Required = true
	cfg.TLS.CertFile = &certPath
	cfg.TLS.KeyFile = &keyPath

	// Validate only checks logical consistency — it does NOT load the cert.
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
