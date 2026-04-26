// Package errors defines the ERR#### error code system used throughout SpeechMux Core.
// Error codes are permanently fixed — once assigned, a code's meaning never changes.
package errors

import (
	"errors"
	"fmt"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorCode is a typed string for ERR#### codes.
type ErrorCode string

// Core error codes — ERR1xxx: session/request validation and authentication.
const (
	ErrSessionIDMissing        ErrorCode = "ERR1001"
	ErrSessionIDDuplicate      ErrorCode = "ERR1002"
	ErrVADConfigInvalid        ErrorCode = "ERR1003"
	ErrUnauthenticated         ErrorCode = "ERR1004"
	ErrSessionTokenInvalid     ErrorCode = "ERR1005"
	ErrSessionTimeout          ErrorCode = "ERR1006"
	ErrAudioChunkTooLarge      ErrorCode = "ERR1007"
	ErrVADCapacityFull         ErrorCode = "ERR1008"
	ErrAPIKeyMissing           ErrorCode = "ERR1009"
	ErrDecodeOptionsInvalid    ErrorCode = "ERR1010"
	ErrMaxSessionsExceeded     ErrorCode = "ERR1011"
	ErrCreateSessionRateLimit  ErrorCode = "ERR1012"
	ErrServerShuttingDown      ErrorCode = "ERR1013"
	ErrAuthFailed              ErrorCode = "ERR1014"
	ErrUnsupportedEncoding     ErrorCode = "ERR1015"
	ErrFirstMessageNotConfig   ErrorCode = "ERR1016"
	ErrUnsupportedLanguage     ErrorCode = "ERR1017"
	ErrSessionNotFound                  ErrorCode = "ERR1018"
	ErrResumeTokenInvalid               ErrorCode = "ERR1019"
	ErrEngineEndpointingUnsupported     ErrorCode = "ERR1020"
	ErrHybridNotSupported               ErrorCode = "ERR1021"
)

// ERR2xxx: decode pipeline errors.
const (
	ErrDecodeTimeout          ErrorCode = "ERR2001"
	ErrDecodeTaskFailed       ErrorCode = "ERR2002"
	ErrStreamRateLimit        ErrorCode = "ERR2003"
	ErrStreamBudgetExceeded   ErrorCode = "ERR2004"
	ErrAllPluginsUnavailable  ErrorCode = "ERR2005"
	ErrResultAssemblyFailed   ErrorCode = "ERR2006"
	ErrPartialDecodeTimeout   ErrorCode = "ERR2007"
	ErrGlobalPendingExceeded  ErrorCode = "ERR2008"
)

// ERR3xxx: internal server errors.
const (
	ErrSessionCreateInternal    ErrorCode = "ERR3001"
	ErrStreamProcessingInternal ErrorCode = "ERR3002"
	ErrCodecConversionFailed    ErrorCode = "ERR3003"
	ErrVADStreamConnectFailed   ErrorCode = "ERR3004"
	ErrAudioBufferOverflow      ErrorCode = "ERR3005"
	ErrStreamingEndpointLost   ErrorCode = "ERR3006"
	ErrEngineResponseTimeout   ErrorCode = "ERR3007"
)

// ERR4xxx: admin/HTTP errors.
const (
	ErrAdminDisabled           ErrorCode = "ERR4001"
	ErrModelAlreadyLoaded      ErrorCode = "ERR4002"
	ErrModelUnloadFailed       ErrorCode = "ERR4003"
	ErrAdminTokenInvalid       ErrorCode = "ERR4004"
	ErrModelPathForbidden      ErrorCode = "ERR4005"
	ErrObservabilityTokenInvalid ErrorCode = "ERR4006"
	ErrHTTPRateLimit           ErrorCode = "ERR4007"
	ErrClientIPBlocked         ErrorCode = "ERR4008"
	ErrUnknownModelProfile     ErrorCode = "ERR4009"
)

// ErrorSpec defines the mapping from an ErrorCode to gRPC and HTTP statuses.
type ErrorSpec struct {
	Code       ErrorCode
	GRPCCode   codes.Code
	HTTPStatus int
	Message    string
}

// errorSpecs is the authoritative mapping table. Never change existing entries.
var errorSpecs = map[ErrorCode]ErrorSpec{
	ErrSessionIDMissing:          {ErrSessionIDMissing, codes.InvalidArgument, http.StatusBadRequest, "session_id is required"},
	ErrSessionIDDuplicate:        {ErrSessionIDDuplicate, codes.AlreadyExists, http.StatusConflict, "session_id already exists"},
	ErrVADConfigInvalid:          {ErrVADConfigInvalid, codes.InvalidArgument, http.StatusBadRequest, "VAD config invalid"},
	ErrUnauthenticated:           {ErrUnauthenticated, codes.Unauthenticated, http.StatusUnauthorized, "session_config not sent"},
	ErrSessionTokenInvalid:       {ErrSessionTokenInvalid, codes.PermissionDenied, http.StatusForbidden, "session token invalid"},
	ErrSessionTimeout:            {ErrSessionTimeout, codes.DeadlineExceeded, http.StatusGatewayTimeout, "session timed out"},
	ErrAudioChunkTooLarge:        {ErrAudioChunkTooLarge, codes.InvalidArgument, http.StatusBadRequest, "audio chunk too large"},
	ErrVADCapacityFull:           {ErrVADCapacityFull, codes.ResourceExhausted, http.StatusServiceUnavailable, "VAD capacity full"},
	ErrAPIKeyMissing:             {ErrAPIKeyMissing, codes.Unauthenticated, http.StatusUnauthorized, "API key required"},
	ErrDecodeOptionsInvalid:      {ErrDecodeOptionsInvalid, codes.InvalidArgument, http.StatusBadRequest, "invalid decode options"},
	ErrMaxSessionsExceeded:       {ErrMaxSessionsExceeded, codes.ResourceExhausted, http.StatusServiceUnavailable, "max sessions exceeded"},
	ErrCreateSessionRateLimit:    {ErrCreateSessionRateLimit, codes.ResourceExhausted, http.StatusTooManyRequests, "create session rate limit"},
	ErrServerShuttingDown:        {ErrServerShuttingDown, codes.Unavailable, http.StatusServiceUnavailable, "server is shutting down"},
	ErrAuthFailed:                {ErrAuthFailed, codes.Unauthenticated, http.StatusUnauthorized, "authentication failed"},
	ErrUnsupportedEncoding:       {ErrUnsupportedEncoding, codes.InvalidArgument, http.StatusBadRequest, "unsupported audio encoding"},
	ErrFirstMessageNotConfig:     {ErrFirstMessageNotConfig, codes.InvalidArgument, http.StatusBadRequest, "first message must be session_config"},
	ErrUnsupportedLanguage:       {ErrUnsupportedLanguage, codes.InvalidArgument, http.StatusBadRequest, "unsupported language code"},
	ErrSessionNotFound:                  {ErrSessionNotFound, codes.NotFound, http.StatusNotFound, "session not found or expired"},
	ErrResumeTokenInvalid:               {ErrResumeTokenInvalid, codes.PermissionDenied, http.StatusForbidden, "resume token invalid or session already active"},
	ErrEngineEndpointingUnsupported:     {ErrEngineEndpointingUnsupported, codes.InvalidArgument, http.StatusBadRequest, "engine does not support requested endpointing mode"},
	ErrHybridNotSupported:               {ErrHybridNotSupported, codes.Unimplemented, http.StatusNotImplemented, "hybrid endpointing is not supported in this release"},

	ErrDecodeTimeout:             {ErrDecodeTimeout, codes.DeadlineExceeded, http.StatusGatewayTimeout, "decode timed out"},
	ErrDecodeTaskFailed:          {ErrDecodeTaskFailed, codes.Internal, http.StatusInternalServerError, "decode task failed"},
	ErrStreamRateLimit:           {ErrStreamRateLimit, codes.ResourceExhausted, http.StatusTooManyRequests, "stream rate limit exceeded"},
	ErrStreamBudgetExceeded:      {ErrStreamBudgetExceeded, codes.ResourceExhausted, http.StatusTooManyRequests, "stream audio budget exceeded"},
	ErrAllPluginsUnavailable:     {ErrAllPluginsUnavailable, codes.Unavailable, http.StatusServiceUnavailable, "no healthy inference plugin available"},
	ErrResultAssemblyFailed:      {ErrResultAssemblyFailed, codes.Internal, http.StatusInternalServerError, "result assembly failed"},
	ErrPartialDecodeTimeout:      {ErrPartialDecodeTimeout, codes.DeadlineExceeded, http.StatusGatewayTimeout, "partial decode timed out"},
	ErrGlobalPendingExceeded:     {ErrGlobalPendingExceeded, codes.ResourceExhausted, http.StatusServiceUnavailable, "global pending decode limit exceeded"},

	ErrSessionCreateInternal:     {ErrSessionCreateInternal, codes.Unknown, http.StatusInternalServerError, "unexpected error creating session"},
	ErrStreamProcessingInternal:  {ErrStreamProcessingInternal, codes.Unknown, http.StatusInternalServerError, "unexpected error in stream processing"},
	ErrCodecConversionFailed:     {ErrCodecConversionFailed, codes.Internal, http.StatusInternalServerError, "codec conversion failed"},
	ErrVADStreamConnectFailed:    {ErrVADStreamConnectFailed, codes.Internal, http.StatusInternalServerError, "VAD stream connect failed"},
	ErrAudioBufferOverflow:       {ErrAudioBufferOverflow, codes.Internal, http.StatusInternalServerError, "audio ring buffer overflow"},
	ErrStreamingEndpointLost:    {ErrStreamingEndpointLost, codes.Unavailable, http.StatusServiceUnavailable, "streaming inference endpoint lost mid-session"},
	ErrEngineResponseTimeout:    {ErrEngineResponseTimeout, codes.DeadlineExceeded, http.StatusGatewayTimeout, "streaming engine response timeout"},

	ErrAdminDisabled:             {ErrAdminDisabled, codes.Unimplemented, http.StatusNotImplemented, "admin API disabled"},
	ErrModelAlreadyLoaded:        {ErrModelAlreadyLoaded, codes.AlreadyExists, http.StatusConflict, "model already loaded"},
	ErrModelUnloadFailed:         {ErrModelUnloadFailed, codes.FailedPrecondition, http.StatusBadRequest, "model unload failed"},
	ErrAdminTokenInvalid:         {ErrAdminTokenInvalid, codes.Unauthenticated, http.StatusUnauthorized, "admin token invalid"},
	ErrModelPathForbidden:        {ErrModelPathForbidden, codes.PermissionDenied, http.StatusForbidden, "model path forbidden"},
	ErrObservabilityTokenInvalid: {ErrObservabilityTokenInvalid, codes.Unauthenticated, http.StatusUnauthorized, "observability token invalid"},
	ErrHTTPRateLimit:             {ErrHTTPRateLimit, codes.ResourceExhausted, http.StatusTooManyRequests, "HTTP rate limit exceeded"},
	ErrClientIPBlocked:           {ErrClientIPBlocked, codes.PermissionDenied, http.StatusForbidden, "client IP blocked"},
	ErrUnknownModelProfile:       {ErrUnknownModelProfile, codes.InvalidArgument, http.StatusBadRequest, "unknown model profile"},
}

// Spec returns the ErrorSpec for the given code.
// Panics if the code is not registered — this is a programmer error.
func Spec(code ErrorCode) ErrorSpec {
	s, ok := errorSpecs[code]
	if !ok {
		panic(fmt.Sprintf("speechmux/errors: unregistered error code %q", code))
	}
	return s
}

// STTError is a structured error carrying an ERR#### code.
type STTError struct {
	spec    ErrorSpec
	detail  string
}

// New creates an STTError for the given code with an optional detail message.
func New(code ErrorCode, detail string) *STTError {
	return &STTError{spec: Spec(code), detail: detail}
}

// Error implements the error interface.
func (e *STTError) Error() string {
	if e.detail != "" {
		return fmt.Sprintf("%s %s: %s", e.spec.Code, e.spec.Message, e.detail)
	}
	return fmt.Sprintf("%s %s", e.spec.Code, e.spec.Message)
}

// Code returns the ErrorCode.
func (e *STTError) Code() ErrorCode { return e.spec.Code }

// GRPCStatus converts the error into a gRPC status error.
func (e *STTError) GRPCStatus() *status.Status {
	msg := fmt.Sprintf("%s %s", e.spec.Code, e.spec.Message)
	if e.detail != "" {
		msg = fmt.Sprintf("%s %s: %s", e.spec.Code, e.spec.Message, e.detail)
	}
	return status.New(e.spec.GRPCCode, msg)
}

// ToGRPC returns a gRPC-compatible error.
func (e *STTError) ToGRPC() error {
	return e.GRPCStatus().Err()
}

// HTTPStatus returns the HTTP status code.
func (e *STTError) HTTPStatus() int { return e.spec.HTTPStatus }

// WireError holds the fields needed to serialise a pipeline error to the client —
// as a gRPC StreamError frame or a WebSocket {"type":"error"} message.
type WireError struct {
	Code      string
	Message   string
	Retryable bool
}

// ToErrorSpec converts a Go error into a WireError for transmission to clients.
// If err wraps an *STTError the registered code, message, and retryability are
// used. All other errors fall back to ErrStreamProcessingInternal (ERR3002).
func ToErrorSpec(err error) WireError {
	var sttErr *STTError
	if errors.As(err, &sttErr) {
		retryable := sttErr.spec.GRPCCode == codes.Unavailable ||
			sttErr.spec.GRPCCode == codes.ResourceExhausted
		return WireError{
			Code:      string(sttErr.spec.Code),
			Message:   sttErr.spec.Message,
			Retryable: retryable,
		}
	}
	fb := Spec(ErrStreamProcessingInternal)
	return WireError{Code: string(fb.Code), Message: fb.Message}
}
