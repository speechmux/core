// Package transport contains network transport implementations for SpeechMux Core.
package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strings"

	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/session"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// SessionProcessor processes a session's audio pipeline. Implemented by
// stream.StreamProcessor; the interface breaks the import cycle.
type SessionProcessor interface {
	ProcessSession(ctx context.Context, sess *session.Session) error
}

// GRPCServer wraps the gRPC server and implements the STTService.
// It exposes a single StreamingRecognize bidi-stream RPC.
type GRPCServer struct {
	clientpb.UnimplementedSTTServiceServer
	server    *grpc.Server
	port      int
	sessions  *session.Manager
	processor SessionProcessor // nil when VAD plugin is not configured
}

// NewGRPCServer creates a GRPCServer listening on the given port.
// processor may be nil (audio is discarded when not configured).
// tlsCfg may be nil for plaintext; when non-nil the server requires TLS for all connections.
func NewGRPCServer(port int, sm *session.Manager, processor SessionProcessor, tlsCfg *tls.Config) *GRPCServer {
	s := &GRPCServer{port: port, sessions: sm, processor: processor}
	var opts []grpc.ServerOption
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	s.server = grpc.NewServer(opts...)
	clientpb.RegisterSTTServiceServer(s.server, s)
	return s
}

// Serve starts the gRPC listener. It blocks until the server stops.
func (s *GRPCServer) Serve(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc: listen %s: %w", addr, err)
	}
	slog.Info("gRPC server listening", "addr", addr)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.server.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		// Caller cancelled — graceful stop is handled by GracefulStop/Stop.
		return nil
	case err := <-errCh:
		return err
	}
}

// GracefulStop signals the gRPC server to stop accepting new RPCs while
// letting existing streams complete. Called in Phase 1 of graceful shutdown.
func (s *GRPCServer) GracefulStop() {
	s.server.GracefulStop()
}

// Stop forcefully terminates all connections. Called in Phase 3 of graceful shutdown.
func (s *GRPCServer) Stop() {
	s.server.Stop()
}

// StreamingRecognize is the single bidi-stream RPC that handles an entire session lifecycle.
//
// Protocol:
//   - First message must be oneof session_config; otherwise ERR1016 is returned.
//   - Subsequent messages are audio or signal frames.
func (s *GRPCServer) StreamingRecognize(stream clientpb.STTService_StreamingRecognizeServer) error {
	ctx := stream.Context()

	// 1. Receive first message — must be session_config.
	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	sessionConfig := firstMsg.GetSessionConfig()
	if sessionConfig == nil {
		return sttErrors.New(sttErrors.ErrFirstMessageNotConfig, "").ToGRPC()
	}

	// 2. Create session (validates auth, rate limits, capacity).
	peerIP := extractPeerIP(ctx)
	grpcMD := extractMetadata(ctx)
	sess, err := s.sessions.CreateSession(ctx, sessionConfig, peerIP, grpcMD)
	if err != nil {
		return err
	}
	defer s.sessions.CloseSession(sess.ID)

	// 3. Send SessionCreated with negotiated settings.
	if err := stream.Send(&clientpb.StreamingRecognizeResponse{
		StreamingResponse: &clientpb.StreamingRecognizeResponse_SessionCreated{
			SessionCreated: &clientpb.SessionCreated{
				SessionId: sess.ID,
				NegotiatedVad: &clientpb.VADConfig{
					SilenceDuration: sess.Info.VADSilence,
					Threshold:       sess.Info.VADThreshold,
				},
			},
		},
	}); err != nil {
		return err
	}
	slog.Info("session started", "session_id", sess.ID, "peer_ip", peerIP)

	// 4. Start stream processor (if configured) in a background goroutine.
	// It reads from sess.AudioInCh for the lifetime of the session.
	if s.processor != nil {
		go func() {
			if err := s.processor.ProcessSession(sess.Context(), sess); err != nil {
				slog.Warn("stream processor error", "session_id", sess.ID, "error", err)
			}
		}()
	}

	// 4b. Stream recognition results from the decode pipeline back to the client.
	// resultsDone is closed when the goroutine exits (ResultCh closed or stream error).
	resultsDone := make(chan struct{})
	go func() {
		defer close(resultsDone)
		for {
			select {
			case result, ok := <-sess.ResultCh:
				if !ok {
					return
				}
				if err := stream.Send(&clientpb.StreamingRecognizeResponse{
					StreamingResponse: &clientpb.StreamingRecognizeResponse_Result{
						Result: result,
					},
				}); err != nil {
					slog.Warn("send result failed", "session_id", sess.ID, "error", err)
					return
				}
			case <-sess.Context().Done():
				// Drain any results already buffered before exiting.
				for {
					select {
					case result, ok := <-sess.ResultCh:
						if !ok {
							return
						}
						_ = stream.Send(&clientpb.StreamingRecognizeResponse{
							StreamingResponse: &clientpb.StreamingRecognizeResponse_Result{
								Result: result,
							},
						})
					default:
						return
					}
				}
			}
		}
	}()

	// 5. Receive audio / signal messages and forward to the session pipeline.
recvLoop:
	for {
		msg, err := stream.Recv()
		if err != nil {
			break recvLoop // io.EOF or client disconnect
		}
		switch v := msg.GetStreamingRequest().(type) {
		case *clientpb.StreamingRecognizeRequest_Audio:
			sess.TouchActivity()
			if s.processor != nil {
				select {
				case sess.AudioInCh <- v.Audio:
				case <-sess.Context().Done():
					break recvLoop
				}
			}
		case *clientpb.StreamingRecognizeRequest_Signal:
			if v.Signal.GetIsLast() {
				// Client has sent all audio — signal the pipeline to flush.
				sess.SignalAudioEnd()
			}
		}
	}

	// Wait for all results to be forwarded to the client before closing the stream.
	// In the disconnect case sess.Context() is cancelled quickly, so this is short.
	if s.processor != nil {
		select {
		case <-resultsDone:
		case <-stream.Context().Done():
		}
	}
	return nil
}

// extractPeerIP returns the client IP from the gRPC stream context.
func extractPeerIP(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	addr := p.Addr.String()
	// addr is "ip:port" — strip the port.
	if idx := strings.LastIndex(addr, ":"); idx >= 0 {
		return addr[:idx]
	}
	return addr
}

// extractMetadata flattens gRPC incoming metadata into a lowercase string map.
func extractMetadata(ctx context.Context) map[string]string {
	md, _ := metadata.FromIncomingContext(ctx)
	result := make(map[string]string, len(md))
	for k, vs := range md {
		if len(vs) > 0 {
			result[strings.ToLower(k)] = vs[0]
		}
	}
	return result
}
