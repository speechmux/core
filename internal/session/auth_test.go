package session_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/session"
)

func TestAuthenticate_None(t *testing.T) {
	cfg := config.AuthConfig{AuthProfile: "none"}
	if err := session.Authenticate(cfg, "sess1", "", nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAuthenticate_RequireAPIKey_Missing(t *testing.T) {
	cfg := config.AuthConfig{RequireAPIKey: true}
	if err := session.Authenticate(cfg, "sess1", "", nil); err == nil {
		t.Error("expected error for missing API key")
	}
}

func TestAuthenticate_RequireAPIKey_Present(t *testing.T) {
	cfg := config.AuthConfig{RequireAPIKey: true}
	if err := session.Authenticate(cfg, "sess1", "mykey", nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAuthenticate_APIKeyProfile_Missing(t *testing.T) {
	cfg := config.AuthConfig{AuthProfile: "api_key"}
	if err := session.Authenticate(cfg, "sess1", "", nil); err == nil {
		t.Error("expected error")
	}
}

func signedToken(secret, sessionID string, tsUnix int64) map[string]string {
	payload := fmt.Sprintf("%s:%d", sessionID, tsUnix)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))
	return map[string]string{
		"authorization": "Bearer " + sig,
		"x-stt-auth-ts": fmt.Sprintf("%d", tsUnix),
	}
}

func TestAuthenticate_SignedToken_Valid(t *testing.T) {
	cfg := config.AuthConfig{
		AuthProfile: "signed_token",
		AuthSecret:  "topsecret",
		AuthTTLSec:  60,
	}
	md := signedToken("topsecret", "sess1", time.Now().Unix())
	if err := session.Authenticate(cfg, "sess1", "", md); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAuthenticate_SignedToken_BadSignature(t *testing.T) {
	cfg := config.AuthConfig{
		AuthProfile: "signed_token",
		AuthSecret:  "topsecret",
		AuthTTLSec:  60,
	}
	md := signedToken("wrongsecret", "sess1", time.Now().Unix())
	if err := session.Authenticate(cfg, "sess1", "", md); err == nil {
		t.Error("expected signature mismatch error")
	}
}

func TestAuthenticate_SignedToken_Expired(t *testing.T) {
	cfg := config.AuthConfig{
		AuthProfile: "signed_token",
		AuthSecret:  "topsecret",
		AuthTTLSec:  10,
	}
	oldTS := time.Now().Unix() - 60
	md := signedToken("topsecret", "sess1", oldTS)
	if err := session.Authenticate(cfg, "sess1", "", md); err == nil {
		t.Error("expected expiry error")
	}
}

func TestAuthenticate_SignedToken_NoSecret(t *testing.T) {
	cfg := config.AuthConfig{
		AuthProfile: "signed_token",
		AuthSecret:  "",
	}
	md := signedToken("topsecret", "sess1", time.Now().Unix())
	if err := session.Authenticate(cfg, "sess1", "", md); err == nil {
		t.Error("expected error for missing secret")
	}
}
