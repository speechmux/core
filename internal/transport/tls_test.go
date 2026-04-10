package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// generateTestTLSConfig creates a self-signed ECDSA cert for 127.0.0.1,
// writes PEM files to a temp directory (cleaned up via t.Cleanup), and returns
// a *tls.Config suitable for use as a server TLS config in tests.
func generateTestTLSConfig(t *testing.T) *tls.Config {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "speechmux-transport-test"},
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

	certPath := filepath.Join(dir, "cert.pem")
	cf, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	_ = pem.Encode(cf, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	cf.Close()

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPath := filepath.Join(dir, "key.pem")
	kf, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	_ = pem.Encode(kf, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	kf.Close()

	tlsCfg, err := config.BuildTLSConfig(config.TLSConfig{
		CertFile: &certPath,
		KeyFile:  &keyPath,
	})
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}
	return tlsCfg
}

// clientTLSConfigTrustingServer builds a *tls.Config that trusts only the
// self-signed certificate embedded in the server TLS config. Used in tests
// to avoid relying on the system cert pool.
func clientTLSConfigTrustingServer(serverTLSCfg *tls.Config) *tls.Config {
	pool := x509.NewCertPool()
	cert, err := x509.ParseCertificate(serverTLSCfg.Certificates[0].Certificate[0])
	if err != nil {
		panic(fmt.Sprintf("parse test cert: %v", err))
	}
	pool.AddCert(cert)
	return &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}
}

// ── HTTP TLS tests ────────────────────────────────────────────────────────────

// TestHTTPServer_TLS_HealthEndpoint verifies that the HTTP server responds
// correctly over HTTPS when configured with a TLS cert.
func TestHTTPServer_TLS_HealthEndpoint(t *testing.T) {
	serverTLSCfg := generateTestTLSConfig(t)
	hs := NewHTTPServer(0, nil, nil, nil, "", serverTLSCfg, nil, HTTPTimeouts{})

	// Bind a real listener to capture the OS-assigned port.
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := rawLis.Addr().String()
	tlsLis := tls.NewListener(rawLis, serverTLSCfg)

	go func() {
		if serveErr := hs.server.Serve(tlsLis); serveErr != nil && serveErr != http.ErrServerClosed {
			t.Logf("server error: %v", serveErr)
		}
	}()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: clientTLSConfigTrustingServer(serverTLSCfg),
		},
	}

	resp, err := client.Get("https://" + addr + "/health")
	if err != nil {
		t.Fatalf("HTTPS GET /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if resp.TLS == nil {
		t.Error("expected resp.TLS to be non-nil for HTTPS connection")
	}
}

// TestHTTPServer_TLS_RejectsUntrustedCert verifies that an HTTPS client with
// an empty trust pool cannot connect (cert verification fails).
func TestHTTPServer_TLS_RejectsUntrustedCert(t *testing.T) {
	serverTLSCfg := generateTestTLSConfig(t)

	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := rawLis.Addr().String()
	tlsLis := tls.NewListener(rawLis, serverTLSCfg)

	hs := NewHTTPServer(0, nil, nil, nil, "", serverTLSCfg, nil, HTTPTimeouts{})
	go func() { _ = hs.server.Serve(tlsLis) }()
	t.Cleanup(func() { _ = hs.server.Close() })

	// HTTPS client with an empty trust pool — self-signed cert is not trusted.
	untrustedClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    x509.NewCertPool(), // empty: trusts nothing
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	_, tlsErr := untrustedClient.Get("https://" + addr + "/health")
	if tlsErr == nil {
		t.Fatal("expected TLS verification error for untrusted self-signed cert")
	}
}

// ── gRPC TLS tests ────────────────────────────────────────────────────────────

// TestGRPCServer_TLS verifies that a TLS gRPC client can connect, complete a
// handshake, and exchange an RPC with a TLS-enabled GRPCServer.
func TestGRPCServer_TLS(t *testing.T) {
	serverTLSCfg := generateTestTLSConfig(t)

	sm := newTestSessionManager()
	srv := NewGRPCServer(0, sm, nil, serverTLSCfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	go func() { _ = srv.server.Serve(lis) }()
	t.Cleanup(func() { srv.Stop() })

	clientTLSCfg := clientTLSConfigTrustingServer(serverTLSCfg)
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLSCfg)),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := clientpb.NewSTTServiceClient(conn)
	stream, err := client.StreamingRecognize(context.Background())
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}
	defer stream.CloseSend()

	// Send a non-config first message to trigger ERR1016.
	// A gRPC error response confirms the TLS handshake succeeded.
	_ = stream.Send(&clientpb.StreamingRecognizeRequest{
		StreamingRequest: &clientpb.StreamingRecognizeRequest_Audio{
			Audio: []byte("hi"),
		},
	})
	if _, recvErr := stream.Recv(); recvErr == nil {
		t.Fatal("expected ERR1016 for non-config first message")
	}
}

// TestGRPCServer_TLS_RejectsPlaintext verifies that a plaintext gRPC client
// cannot connect to a TLS-only gRPC server.
func TestGRPCServer_TLS_RejectsPlaintext(t *testing.T) {
	serverTLSCfg := generateTestTLSConfig(t)

	sm := newTestSessionManager()
	srv := NewGRPCServer(0, sm, nil, serverTLSCfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	go func() { _ = srv.server.Serve(lis) }()
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := clientpb.NewSTTServiceClient(conn)
	stream, err := client.StreamingRecognize(context.Background())
	if err != nil {
		// Handshake failed before stream opened — expected.
		return
	}
	defer stream.CloseSend()

	_ = stream.Send(&clientpb.StreamingRecognizeRequest{
		StreamingRequest: &clientpb.StreamingRecognizeRequest_Signal{
			Signal: &clientpb.StreamSignal{IsLast: true},
		},
	})
	if _, recvErr := stream.Recv(); recvErr == nil {
		t.Fatal("expected handshake failure: plaintext client against TLS server")
	}
}
