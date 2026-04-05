package errors_test

// contract_test.go verifies that the ERR#### error code mapping table is
// internally consistent and complete. These tests act as a compatibility
// contract: if a code's gRPC status or HTTP status is accidentally changed,
// the test will catch it.

import (
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"

	sttErrors "github.com/speechmux/core/internal/errors"
)

// contractEntry defines the expected mapping for one error code.
type contractEntry struct {
	code       sttErrors.ErrorCode
	grpcCode   codes.Code
	httpStatus int
}

// errorCodeContract is the authoritative mapping table.
// Any change here is a breaking protocol change and must be deliberate.
var errorCodeContract = []contractEntry{
	// ERR1xxx — session/request validation
	{sttErrors.ErrSessionIDMissing, codes.InvalidArgument, http.StatusBadRequest},
	{sttErrors.ErrSessionIDDuplicate, codes.AlreadyExists, http.StatusConflict},
	{sttErrors.ErrVADConfigInvalid, codes.InvalidArgument, http.StatusBadRequest},
	{sttErrors.ErrUnauthenticated, codes.Unauthenticated, http.StatusUnauthorized},
	{sttErrors.ErrSessionTokenInvalid, codes.PermissionDenied, http.StatusForbidden},
	{sttErrors.ErrSessionTimeout, codes.DeadlineExceeded, http.StatusGatewayTimeout},
	{sttErrors.ErrAudioChunkTooLarge, codes.InvalidArgument, http.StatusBadRequest},
	{sttErrors.ErrVADCapacityFull, codes.ResourceExhausted, http.StatusServiceUnavailable},
	{sttErrors.ErrAPIKeyMissing, codes.Unauthenticated, http.StatusUnauthorized},
	{sttErrors.ErrDecodeOptionsInvalid, codes.InvalidArgument, http.StatusBadRequest},
	{sttErrors.ErrMaxSessionsExceeded, codes.ResourceExhausted, http.StatusServiceUnavailable},
	{sttErrors.ErrCreateSessionRateLimit, codes.ResourceExhausted, http.StatusTooManyRequests},
	{sttErrors.ErrServerShuttingDown, codes.Unavailable, http.StatusServiceUnavailable},
	{sttErrors.ErrAuthFailed, codes.Unauthenticated, http.StatusUnauthorized},
	{sttErrors.ErrUnsupportedEncoding, codes.InvalidArgument, http.StatusBadRequest},
	{sttErrors.ErrFirstMessageNotConfig, codes.InvalidArgument, http.StatusBadRequest},
	{sttErrors.ErrUnsupportedLanguage, codes.InvalidArgument, http.StatusBadRequest},

	// ERR2xxx — decode pipeline
	{sttErrors.ErrDecodeTimeout, codes.DeadlineExceeded, http.StatusGatewayTimeout},
	{sttErrors.ErrDecodeTaskFailed, codes.Internal, http.StatusInternalServerError},
	{sttErrors.ErrStreamRateLimit, codes.ResourceExhausted, http.StatusTooManyRequests},
	{sttErrors.ErrStreamBudgetExceeded, codes.ResourceExhausted, http.StatusTooManyRequests},
	{sttErrors.ErrAllPluginsUnavailable, codes.Unavailable, http.StatusServiceUnavailable},
	{sttErrors.ErrResultAssemblyFailed, codes.Internal, http.StatusInternalServerError},
	{sttErrors.ErrPartialDecodeTimeout, codes.DeadlineExceeded, http.StatusGatewayTimeout},
	{sttErrors.ErrGlobalPendingExceeded, codes.ResourceExhausted, http.StatusServiceUnavailable},

	// ERR3xxx — internal server errors
	{sttErrors.ErrSessionCreateInternal, codes.Unknown, http.StatusInternalServerError},
	{sttErrors.ErrStreamProcessingInternal, codes.Unknown, http.StatusInternalServerError},
	{sttErrors.ErrCodecConversionFailed, codes.Internal, http.StatusInternalServerError},
	{sttErrors.ErrVADStreamConnectFailed, codes.Internal, http.StatusInternalServerError},
	{sttErrors.ErrAudioBufferOverflow, codes.Internal, http.StatusInternalServerError},

	// ERR4xxx — admin/HTTP
	{sttErrors.ErrAdminDisabled, codes.Unimplemented, http.StatusNotImplemented},
	{sttErrors.ErrModelAlreadyLoaded, codes.AlreadyExists, http.StatusConflict},
	{sttErrors.ErrModelUnloadFailed, codes.FailedPrecondition, http.StatusBadRequest},
	{sttErrors.ErrAdminTokenInvalid, codes.Unauthenticated, http.StatusUnauthorized},
	{sttErrors.ErrModelPathForbidden, codes.PermissionDenied, http.StatusForbidden},
	{sttErrors.ErrObservabilityTokenInvalid, codes.Unauthenticated, http.StatusUnauthorized},
	{sttErrors.ErrHTTPRateLimit, codes.ResourceExhausted, http.StatusTooManyRequests},
	{sttErrors.ErrClientIPBlocked, codes.PermissionDenied, http.StatusForbidden},
	{sttErrors.ErrUnknownModelProfile, codes.InvalidArgument, http.StatusBadRequest},
}

// TestErrorCodeContract verifies every code's gRPC and HTTP status mappings.
func TestErrorCodeContract(t *testing.T) {
	for _, entry := range errorCodeContract {
		e := sttErrors.New(entry.code, "test")

		gotGRPC := e.GRPCStatus().Code()
		if gotGRPC != entry.grpcCode {
			t.Errorf("%s: gRPC code = %v, want %v", entry.code, gotGRPC, entry.grpcCode)
		}

		gotHTTP := e.HTTPStatus()
		if gotHTTP != entry.httpStatus {
			t.Errorf("%s: HTTP status = %d, want %d", entry.code, gotHTTP, entry.httpStatus)
		}
	}
}

// TestErrorCodeContract_AllCodesRegistered ensures Spec() does not panic for any
// code in the contract table (i.e., every code has a registered entry).
func TestErrorCodeContract_AllCodesRegistered(t *testing.T) {
	for _, entry := range errorCodeContract {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: Spec() panicked: %v", entry.code, r)
				}
			}()
			_ = sttErrors.Spec(entry.code)
		}()
	}
}

// TestSTTError_ToGRPC_IncludesCode verifies that the gRPC error message starts
// with the ERR#### prefix (clients rely on this for error code extraction).
func TestSTTError_ToGRPC_IncludesCode(t *testing.T) {
	e := sttErrors.New(sttErrors.ErrDecodeTimeout, "took too long")
	grpcErr := e.ToGRPC()
	msg := grpcErr.Error()

	if len(msg) < 7 {
		t.Fatalf("gRPC error message too short: %q", msg)
	}
	// The message format is "rpc error: code = DeadlineExceeded desc = ERR2001 ..."
	if !containsSubstring(msg, "ERR2001") {
		t.Errorf("gRPC error message does not contain ERR2001: %q", msg)
	}
}

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && func() bool {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()
}
