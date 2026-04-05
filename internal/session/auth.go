package session

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/config"
)

// Authenticate validates the auth fields of an incoming SessionConfig.
// It returns an *errors.STTError if validation fails, nil on success.
func Authenticate(cfg config.AuthConfig, sessionID, apiKey string, md map[string]string) error {
	profile := normalizeAuthProfile(cfg.AuthProfile)

	switch profile {
	case authProfileNone:
		if cfg.RequireAPIKey && apiKey == "" {
			return sttErrors.New(sttErrors.ErrAPIKeyMissing, "")
		}
		return nil

	case authProfileAPIKey:
		if apiKey == "" {
			return sttErrors.New(sttErrors.ErrAPIKeyMissing, "")
		}
		return nil

	case authProfileSignedToken:
		return validateSignedToken(cfg, sessionID, md)

	default:
		return sttErrors.New(sttErrors.ErrAuthFailed, fmt.Sprintf("unknown auth profile: %s", profile))
	}
}

const (
	authProfileNone        = "none"
	authProfileAPIKey      = "api_key"
	authProfileSignedToken = "signed_token"
)

var authProfileAliases = map[string]string{
	"none": authProfileNone, "off": authProfileNone,
	"false": authProfileNone, "0": authProfileNone,
	"api_key": authProfileAPIKey, "api-key": authProfileAPIKey, "apikey": authProfileAPIKey,
	"signed_token": authProfileSignedToken, "signed": authProfileSignedToken,
	"signature": authProfileSignedToken, "hmac": authProfileSignedToken,
}

func normalizeAuthProfile(raw string) string {
	key := strings.ToLower(strings.TrimSpace(raw))
	if key == "" {
		return authProfileNone
	}
	if v, ok := authProfileAliases[key]; ok {
		return v
	}
	return key
}

// validateSignedToken checks HMAC-SHA256 signed-token auth.
// Expected: the client provides:
//   - x-stt-auth-ts (or x-auth-ts): Unix timestamp (seconds or milliseconds)
//   - authorization: Bearer <hex_signature>  (or x-auth-sig)
//
// Payload signed: "{session_id}:{timestamp}"
func validateSignedToken(cfg config.AuthConfig, sessionID string, md map[string]string) error {
	secret := strings.TrimSpace(cfg.AuthSecret)
	if secret == "" {
		return sttErrors.New(sttErrors.ErrAuthFailed, "auth secret not configured")
	}

	tsRaw := metadataValue(md, "x-stt-auth-ts", "x-auth-ts", "x-auth-timestamp")
	sigRaw := extractSignature(md)

	// Legacy format: "authorization: <ts>:<sig>"
	if (tsRaw == "" || sigRaw == "") && md["authorization"] != "" {
		raw := strings.TrimSpace(md["authorization"])
		if parts := strings.SplitN(raw, ":", 2); len(parts) == 2 {
			if tsRaw == "" {
				tsRaw = strings.TrimSpace(parts[0])
			}
			if sigRaw == "" || strings.Contains(sigRaw, ":") {
				sigRaw = strings.TrimSpace(parts[1])
			}
		}
	}

	if tsRaw == "" || sigRaw == "" {
		return sttErrors.New(sttErrors.ErrAuthFailed, "missing timestamp or signature")
	}

	tsFloat, err := strconv.ParseFloat(tsRaw, 64)
	if err != nil {
		return sttErrors.New(sttErrors.ErrAuthFailed, "invalid timestamp")
	}
	tsSec := int64(tsFloat)
	if math.Abs(tsFloat) > 1e11 {
		// Millisecond epoch — convert to seconds.
		tsSec = int64(tsFloat / 1000)
	}

	if ttl := cfg.AuthTTLSec; ttl > 0 {
		diff := time.Now().Unix() - tsSec
		if diff < 0 {
			diff = -diff
		}
		if diff > int64(ttl) {
			return sttErrors.New(sttErrors.ErrAuthFailed, "token expired")
		}
	}

	payload := fmt.Sprintf("%s:%d", sessionID, tsSec)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	expected := hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(expected), []byte(sigRaw)) {
		return sttErrors.New(sttErrors.ErrAuthFailed, "signature mismatch")
	}
	return nil
}

func metadataValue(md map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := md[k]; v != "" {
			return v
		}
	}
	return ""
}

func extractSignature(md map[string]string) string {
	if auth := md["authorization"]; auth != "" {
		parts := strings.SplitN(strings.TrimSpace(auth), " ", 2)
		if len(parts) == 2 {
			prefix := strings.ToLower(parts[0])
			if prefix == "bearer" || prefix == "token" || prefix == "signature" || prefix == "hmac" {
				return strings.TrimSpace(parts[1])
			}
		}
		return strings.TrimSpace(auth)
	}
	return metadataValue(md, "x-auth-sig", "x-auth-signature", "x-stt-auth")
}
