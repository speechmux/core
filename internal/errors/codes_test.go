package errors_test

import (
	"net/http"
	"testing"

	sttErrors "github.com/speechmux/core/internal/errors"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	"google.golang.org/grpc/codes"
)

func TestErrorCodeGRPCMapping(t *testing.T) {
	cases := []struct {
		code     sttErrors.ErrorCode
		grpc     codes.Code
		httpCode int
	}{
		{sttErrors.ErrFirstMessageNotConfig, codes.InvalidArgument, http.StatusBadRequest},
		{sttErrors.ErrSessionIDMissing, codes.InvalidArgument, http.StatusBadRequest},
		{sttErrors.ErrSessionIDDuplicate, codes.AlreadyExists, http.StatusConflict},
		{sttErrors.ErrMaxSessionsExceeded, codes.ResourceExhausted, http.StatusServiceUnavailable},
		{sttErrors.ErrServerShuttingDown, codes.Unavailable, http.StatusServiceUnavailable},
		{sttErrors.ErrDecodeTimeout, codes.DeadlineExceeded, http.StatusGatewayTimeout},
		{sttErrors.ErrAllPluginsUnavailable, codes.Unavailable, http.StatusServiceUnavailable},
		{sttErrors.ErrAudioBufferOverflow, codes.Internal, http.StatusInternalServerError},
	}

	for _, tc := range cases {
		t.Run(string(tc.code), func(t *testing.T) {
			err := sttErrors.New(tc.code, "")
			if err.HTTPStatus() != tc.httpCode {
				t.Errorf("HTTP: got %d, want %d", err.HTTPStatus(), tc.httpCode)
			}
			st := err.GRPCStatus()
			if st.Code() != tc.grpc {
				t.Errorf("gRPC: got %v, want %v", st.Code(), tc.grpc)
			}
			grpcErr := err.ToGRPC()
			if grpcErr == nil {
				t.Error("ToGRPC returned nil")
			}
		})
	}
}

func TestSTTErrorMessageContainsCode(t *testing.T) {
	err := sttErrors.New(sttErrors.ErrFirstMessageNotConfig, "test detail")
	msg := err.Error()
	if msg == "" {
		t.Fatal("Error() returned empty string")
	}
	// Must contain the code string.
	if len(msg) < len("ERR1016") {
		t.Errorf("error message too short: %q", msg)
	}
}

func TestFromPluginError(t *testing.T) {
	cases := []struct {
		in   commonpb.PluginErrorCode
		want sttErrors.ErrorCode
	}{
		{commonpb.PluginErrorCode_PLUGIN_ERROR_MODEL_LOADING, sttErrors.ErrAllPluginsUnavailable},
		{commonpb.PluginErrorCode_PLUGIN_ERROR_MODEL_OOM, sttErrors.ErrAllPluginsUnavailable},
		{commonpb.PluginErrorCode_PLUGIN_ERROR_INVALID_AUDIO, sttErrors.ErrCodecConversionFailed},
		{commonpb.PluginErrorCode_PLUGIN_ERROR_INFERENCE_FAILED, sttErrors.ErrDecodeTaskFailed},
		{commonpb.PluginErrorCode_PLUGIN_ERROR_SESSION_NOT_FOUND, sttErrors.ErrVADStreamConnectFailed},
		{commonpb.PluginErrorCode_PLUGIN_ERROR_CAPACITY_FULL, sttErrors.ErrGlobalPendingExceeded},
	}
	for _, tc := range cases {
		got := sttErrors.FromPluginError(tc.in)
		if got != tc.want {
			t.Errorf("FromPluginError(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
