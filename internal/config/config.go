// Package config defines configuration structures for SpeechMux Core.
// All fields map directly to the YAML structure in config/core.yaml and config/plugins.yaml.
package config

import (
	"crypto/tls"
	"fmt"
	"time"
)

// DecodeConfig holds scheduler capacity settings for batch and streaming decodes.
type DecodeConfig struct {
	MaxPending           int `yaml:"max_pending"`            // batch unary queue depth; 0 = unlimited
	MaxStreamingSessions int `yaml:"max_streaming_sessions"` // streaming session cap; 0 = unlimited
}

// Config is the top-level runtime configuration.
// It is stored behind an atomic.Pointer[Config] for lock-free hot-reload.
type Config struct {
	Server         ServerConfig              `yaml:"server"`
	Stream         StreamConfig              `yaml:"stream"`
	Decode         DecodeConfig              `yaml:"decode"`
	Codec          CodecConfig               `yaml:"codec"`
	RateLimit      RateLimitConfig           `yaml:"rate_limit"`
	Auth           AuthConfig                `yaml:"auth"`
	Storage        StorageConfig             `yaml:"storage"`
	Logging        LoggingConfig             `yaml:"logging"`
	TLS            TLSConfig                 `yaml:"tls"`
	OTel           OTelConfig                `yaml:"otel"`
	DecodeProfiles map[string]DecodeProfile  `yaml:"decode_profiles"`
}

// OTelConfig holds OpenTelemetry tracing settings.
type OTelConfig struct {
	Endpoint    string  `yaml:"endpoint"`     // OTLP gRPC endpoint (host:port); empty = disabled
	ServiceName string  `yaml:"service_name"` // resource service.name attribute
	SampleRate  float64 `yaml:"sample_rate"`  // TraceIDRatioBased rate (0.0–1.0); 1.0 = all
}

// ServerConfig holds network and capacity settings.
type ServerConfig struct {
	GRPCPort          int           `yaml:"grpc_port"`
	HTTPPort          int           `yaml:"http_port"`
	WSPort            int           `yaml:"ws_port"`
	MaxSessions       int           `yaml:"max_sessions"`
	AllowedOrigins    []string      `yaml:"allowed_origins"`
	SessionTimeoutSec time.Duration `yaml:"-"` // parsed from yaml int below
	ShutdownDrainSec  time.Duration `yaml:"-"` // parsed from yaml int below
	// ResumableSessionTimeoutSec is the window a parked session is kept alive
	// after an unexpected WebSocket disconnect. 0 means resume is disabled.
	ResumableSessionTimeoutSec time.Duration `yaml:"-"` // parsed from yaml int below

	// HTTP server connection timeouts (parsed from yaml ints below).
	HTTPReadTimeout     time.Duration `yaml:"-"`
	HTTPWriteTimeout    time.Duration `yaml:"-"`
	HTTPIdleTimeout     time.Duration `yaml:"-"`
	HTTPShutdownTimeout time.Duration `yaml:"-"`

	// Raw YAML integers; converted to time.Duration by Validate().
	SessionTimeoutRaw          int `yaml:"session_timeout_sec"`
	ShutdownDrainRaw           int `yaml:"shutdown_drain_sec"`
	ResumableSessionTimeoutRaw int `yaml:"resumable_session_timeout_sec"`
	HTTPReadTimeoutRaw         int `yaml:"http_read_timeout_sec"`
	HTTPWriteTimeoutRaw        int `yaml:"http_write_timeout_sec"`
	HTTPIdleTimeoutRaw         int `yaml:"http_idle_timeout_sec"`
	HTTPShutdownTimeoutRaw     int `yaml:"http_shutdown_timeout_sec"`
}

// StreamConfig holds audio processing and VAD parameters.
type StreamConfig struct {
	VADSilenceSec            float64 `yaml:"vad_silence_sec"`
	VADThreshold             float64 `yaml:"vad_threshold"`
	SpeechRMSThreshold       float64 `yaml:"speech_rms_threshold"`
	PartialDecodeIntervalSec float64 `yaml:"partial_decode_interval_sec"`
	PartialDecodeWindowSec   float64 `yaml:"partial_decode_window_sec"`
	DecodeTimeoutSec         float64 `yaml:"decode_timeout_sec"`
	MaxBufferSec             float64 `yaml:"max_buffer_sec"`
	BufferOverlapSec         float64 `yaml:"buffer_overlap_sec"`
	EmitFinalOnVAD           bool    `yaml:"emit_final_on_vad"`
	VADFrameTimeoutSec          float64 `yaml:"vad_frame_timeout_sec"`
	EPDHeartbeatIntervalSec     float64 `yaml:"epd_heartbeat_interval_sec"`
	VADWatermarkLagThresholdSec float64 `yaml:"vad_watermark_lag_threshold_sec"`
}

// CircuitBreakerConfig holds circuit-breaker tuning for a plugin endpoint pool.
type CircuitBreakerConfig struct {
	FailureThreshold   int           `yaml:"failure_threshold"`
	HalfOpenTimeoutSec int           `yaml:"half_open_timeout_sec"`
	HalfOpenTimeout    time.Duration `yaml:"-"` // parsed from HalfOpenTimeoutSec by PluginsConfig.Validate()
}

// CodecConfig holds audio codec conversion settings.
type CodecConfig struct {
	TargetSampleRate int `yaml:"target_sample_rate"`
}

// RateLimitConfig holds per-IP and per-key rate limiting parameters.
type RateLimitConfig struct {
	CreateSessionRPS    float64 `yaml:"create_session_rps"`
	CreateSessionBurst  float64 `yaml:"create_session_burst"`
	MaxSessionsPerIP    int     `yaml:"max_sessions_per_ip"`
	MaxSessionsPerKey   int     `yaml:"max_sessions_per_api_key"`
	HTTPRPS             float64 `yaml:"http_rps"`
	HTTPBurst           float64 `yaml:"http_burst"`
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	RequireAPIKey bool   `yaml:"require_api_key"`
	AuthProfile   string `yaml:"auth_profile"` // none | api_key | signed_token
	AuthSecret    string `yaml:"auth_secret"`
	AuthTTLSec    int    `yaml:"auth_ttl_sec"`
}

// StorageConfig holds audio recording settings.
type StorageConfig struct {
	Enabled    bool    `yaml:"enabled"`
	Directory  string  `yaml:"directory"`
	MaxBytes   *int64  `yaml:"max_bytes"`
	MaxFiles   *int    `yaml:"max_files"`
	MaxAgeDays *int    `yaml:"max_age_days"`
}

// LoggingConfig holds log output settings.
type LoggingConfig struct {
	Level string  `yaml:"level"`
	File  *string `yaml:"file"`
}

// TLSConfig holds TLS certificate settings.
type TLSConfig struct {
	CertFile *string `yaml:"cert_file"`
	KeyFile  *string `yaml:"key_file"`
	Required bool    `yaml:"tls_required"`
}

// BuildTLSConfig loads a *tls.Config from the TLSConfig settings.
// Returns nil when CertFile or KeyFile is not set (TLS disabled).
// Returns an error if the cert/key pair cannot be loaded from disk.
func BuildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if cfg.CertFile == nil || cfg.KeyFile == nil {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(*cfg.CertFile, *cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load cert %q / key %q: %w", *cfg.CertFile, *cfg.KeyFile, err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// DecodeProfile holds Whisper decode hyperparameters.
type DecodeProfile struct {
	BeamSize                   int     `yaml:"beam_size"`
	BestOf                     int     `yaml:"best_of"`
	Temperature                float64 `yaml:"temperature"`
	WithoutTimestamps          bool    `yaml:"without_timestamps"`
	CompressionRatioThreshold  float64 `yaml:"compression_ratio_threshold"`
	NoSpeechThreshold          float64 `yaml:"no_speech_threshold"`
}

// PluginsConfig is loaded from config/plugins.yaml.
type PluginsConfig struct {
	VAD       VADPluginConfig       `yaml:"vad"`
	Inference InferencePluginConfig `yaml:"inference"`
}

// VADPluginConfig holds VAD plugin connection settings.
type VADPluginConfig struct {
	Socket                 string               `yaml:"socket"`
	Endpoints              []EndpointConfig     `yaml:"endpoints"`
	HealthCheckIntervalSec int                  `yaml:"health_check_interval_sec"`
	CircuitBreaker         CircuitBreakerConfig `yaml:"circuit_breaker"`
}

// AllEndpoints returns all configured VAD endpoints.
// When Endpoints is non-empty it is returned directly; otherwise the legacy
// Socket field is wrapped as a single endpoint with id "vad".
func (v *VADPluginConfig) AllEndpoints() []EndpointConfig {
	if len(v.Endpoints) > 0 {
		return v.Endpoints
	}
	if v.Socket != "" {
		return []EndpointConfig{{ID: "vad", Socket: v.Socket}}
	}
	return nil
}

// InferencePluginConfig holds inference plugin routing and endpoint settings.
type InferencePluginConfig struct {
	RoutingMode            string               `yaml:"routing_mode"` // round_robin | least_connections | active_standby
	Endpoints              []EndpointConfig     `yaml:"endpoints"`
	HealthCheckIntervalSec int                  `yaml:"health_check_interval_sec"`
	HealthProbeTimeoutSec  int                  `yaml:"health_probe_timeout_sec"`
	CircuitBreaker         CircuitBreakerConfig `yaml:"circuit_breaker"`
}

// EndpointConfig identifies a single inference plugin endpoint.
type EndpointConfig struct {
	ID       string `yaml:"id"`
	Socket   string `yaml:"socket"`
	Priority int    `yaml:"priority"`
}

// Defaults applies default values to fields that are zero.
func (c *Config) Defaults() {
	if c.Server.GRPCPort == 0 {
		c.Server.GRPCPort = 50051
	}
	if c.Server.HTTPPort == 0 {
		c.Server.HTTPPort = 8000
	}
	if c.Server.WSPort == 0 {
		c.Server.WSPort = 8001
	}
	if c.Server.MaxSessions == 0 {
		c.Server.MaxSessions = 50
	}
	if c.Server.SessionTimeoutRaw == 0 {
		c.Server.SessionTimeoutRaw = 60
	}
	if c.Server.ShutdownDrainRaw == 0 {
		c.Server.ShutdownDrainRaw = 30
	}
	if c.Stream.VADSilenceSec == 0 {
		c.Stream.VADSilenceSec = 0.5
	}
	if c.Stream.VADThreshold == 0 {
		c.Stream.VADThreshold = 0.5
	}
	if c.Stream.MaxBufferSec == 0 {
		c.Stream.MaxBufferSec = 20
	}
	if c.Codec.TargetSampleRate == 0 {
		c.Codec.TargetSampleRate = 16000
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Server.HTTPReadTimeoutRaw == 0 {
		c.Server.HTTPReadTimeoutRaw = 10
	}
	if c.Server.HTTPWriteTimeoutRaw == 0 {
		c.Server.HTTPWriteTimeoutRaw = 10
	}
	if c.Server.HTTPIdleTimeoutRaw == 0 {
		c.Server.HTTPIdleTimeoutRaw = 60
	}
	if c.Server.HTTPShutdownTimeoutRaw == 0 {
		c.Server.HTTPShutdownTimeoutRaw = 5
	}
	if c.Stream.VADFrameTimeoutSec == 0 {
		c.Stream.VADFrameTimeoutSec = 3.0
	}
	if c.Stream.VADWatermarkLagThresholdSec == 0 {
		c.Stream.VADWatermarkLagThresholdSec = 5.0
	}
}

// Validate converts raw integer durations to time.Duration and checks logical
// consistency of the config (e.g. TLS required but no cert/key configured).
func (c *Config) Validate() error {
	c.Server.SessionTimeoutSec = time.Duration(c.Server.SessionTimeoutRaw) * time.Second
	c.Server.ShutdownDrainSec = time.Duration(c.Server.ShutdownDrainRaw) * time.Second
	c.Server.ResumableSessionTimeoutSec = time.Duration(c.Server.ResumableSessionTimeoutRaw) * time.Second
	c.Server.HTTPReadTimeout = time.Duration(c.Server.HTTPReadTimeoutRaw) * time.Second
	c.Server.HTTPWriteTimeout = time.Duration(c.Server.HTTPWriteTimeoutRaw) * time.Second
	c.Server.HTTPIdleTimeout = time.Duration(c.Server.HTTPIdleTimeoutRaw) * time.Second
	c.Server.HTTPShutdownTimeout = time.Duration(c.Server.HTTPShutdownTimeoutRaw) * time.Second
	if c.TLS.Required && (c.TLS.CertFile == nil || c.TLS.KeyFile == nil) {
		return fmt.Errorf("tls: tls_required is true but cert_file or key_file is not set")
	}
	return nil
}

// ValidatePlugins converts raw integer durations in PluginsConfig to time.Duration
// and fills in defaults for circuit-breaker settings.
func ValidatePlugins(p *PluginsConfig) {
	validateCB := func(cb *CircuitBreakerConfig, defaultThreshold int, defaultHalfOpen int) {
		if cb.FailureThreshold == 0 {
			cb.FailureThreshold = defaultThreshold
		}
		if cb.HalfOpenTimeoutSec == 0 {
			cb.HalfOpenTimeoutSec = defaultHalfOpen
		}
		cb.HalfOpenTimeout = time.Duration(cb.HalfOpenTimeoutSec) * time.Second
	}
	validateCB(&p.VAD.CircuitBreaker, 5, 30)
	validateCB(&p.Inference.CircuitBreaker, 5, 30)
	if p.Inference.HealthProbeTimeoutSec == 0 {
		p.Inference.HealthProbeTimeoutSec = 5
	}
}
