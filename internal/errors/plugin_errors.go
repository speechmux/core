package errors

import (
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
)

// FromPluginError converts a PluginErrorCode from a VAD or inference plugin
// into the corresponding Core ERR#### code.
// Plugin errors must never be forwarded to clients directly — Core owns the translation.
func FromPluginError(code commonpb.PluginErrorCode) ErrorCode {
	switch code {
	case commonpb.PluginErrorCode_PLUGIN_ERROR_MODEL_LOADING:
		return ErrAllPluginsUnavailable
	case commonpb.PluginErrorCode_PLUGIN_ERROR_MODEL_OOM:
		return ErrAllPluginsUnavailable
	case commonpb.PluginErrorCode_PLUGIN_ERROR_INVALID_AUDIO:
		return ErrCodecConversionFailed
	case commonpb.PluginErrorCode_PLUGIN_ERROR_INFERENCE_FAILED:
		return ErrDecodeTaskFailed
	case commonpb.PluginErrorCode_PLUGIN_ERROR_SESSION_NOT_FOUND:
		return ErrVADStreamConnectFailed
	case commonpb.PluginErrorCode_PLUGIN_ERROR_CAPACITY_FULL:
		return ErrGlobalPendingExceeded
	default:
		return ErrStreamProcessingInternal
	}
}
