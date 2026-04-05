package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/speechmux/core/internal/plugin"
)

// ── stub registry ─────────────────────────────────────────────────────────────

type stubRegistry struct {
	endpoints []plugin.EndpointSummary
	addErr    error
	removeErr error
}

func (s *stubRegistry) AddInferenceEndpoint(id, socket string) error {
	if s.addErr != nil {
		return s.addErr
	}
	s.endpoints = append(s.endpoints, plugin.EndpointSummary{
		ID: id, Socket: socket, CircuitBreaker: "closed",
	})
	return nil
}

func (s *stubRegistry) RemoveInferenceEndpoint(id string) error {
	if s.removeErr != nil {
		return s.removeErr
	}
	for i, ep := range s.endpoints {
		if ep.ID == id {
			s.endpoints = append(s.endpoints[:i], s.endpoints[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("endpoint %q not found", id)
}

func (s *stubRegistry) ListInferenceEndpoints() []plugin.EndpointSummary {
	if s.endpoints == nil {
		return []plugin.EndpointSummary{}
	}
	return s.endpoints
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestHTTPServerWithRegistry(reg PluginRegistry, token string) *HTTPServer {
	return NewHTTPServer(0, nil, nil, nil, token, nil, reg, HTTPTimeouts{})
}

// ── GET /admin/plugins ────────────────────────────────────────────────────────

func TestAdminPluginsList_NoAuth_EmptyList(t *testing.T) {
	reg := &stubRegistry{}
	hs := newTestHTTPServerWithRegistry(reg, "")

	req := httptest.NewRequest(http.MethodGet, "/admin/plugins", nil)
	w := httptest.NewRecorder()
	hs.handleAdminPluginsList(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body struct {
		Inference []plugin.EndpointSummary `json:"inference"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Inference) != 0 {
		t.Errorf("expected empty list, got %v", body.Inference)
	}
}

func TestAdminPluginsList_ReturnsEndpoints(t *testing.T) {
	reg := &stubRegistry{
		endpoints: []plugin.EndpointSummary{
			{ID: "ep1", Socket: "/tmp/ep1.sock", CircuitBreaker: "closed"},
		},
	}
	hs := newTestHTTPServerWithRegistry(reg, "")

	req := httptest.NewRequest(http.MethodGet, "/admin/plugins", nil)
	w := httptest.NewRecorder()
	hs.handleAdminPluginsList(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body struct {
		Inference []plugin.EndpointSummary `json:"inference"`
	}
	_ = json.NewDecoder(w.Body).Decode(&body)
	if len(body.Inference) != 1 || body.Inference[0].ID != "ep1" {
		t.Errorf("unexpected body: %v", body)
	}
}

func TestAdminPluginsList_NilRegistry_Returns501(t *testing.T) {
	hs := newTestHTTPServerWithRegistry(nil, "")
	req := httptest.NewRequest(http.MethodGet, "/admin/plugins", nil)
	w := httptest.NewRecorder()
	hs.handleAdminPluginsList(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Errorf("expected 501, got %d", w.Code)
	}
}

func TestAdminPluginsList_WrongMethod_Returns405(t *testing.T) {
	reg := &stubRegistry{}
	hs := newTestHTTPServerWithRegistry(reg, "")
	req := httptest.NewRequest(http.MethodPost, "/admin/plugins", nil)
	w := httptest.NewRecorder()
	hs.handleAdminPluginsList(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

// ── POST /admin/plugins/inference ────────────────────────────────────────────

func TestAdminRegisterInference_Success(t *testing.T) {
	reg := &stubRegistry{}
	hs := newTestHTTPServerWithRegistry(reg, "")

	body := `{"id":"ep1","socket":"/tmp/ep1.sock"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/plugins/inference",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	hs.adminRegisterInference(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if len(reg.endpoints) != 1 || reg.endpoints[0].ID != "ep1" {
		t.Errorf("endpoint not registered: %v", reg.endpoints)
	}
}

func TestAdminRegisterInference_MissingFields_Returns400(t *testing.T) {
	reg := &stubRegistry{}
	hs := newTestHTTPServerWithRegistry(reg, "")

	for _, body := range []string{
		`{"id":"ep1"}`,          // missing socket
		`{"socket":"/tmp/s.sock"}`, // missing id
		`{}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/admin/plugins/inference",
			bytes.NewBufferString(body))
		w := httptest.NewRecorder()
		hs.adminRegisterInference(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %q: expected 400, got %d", body, w.Code)
		}
	}
}

func TestAdminRegisterInference_AddError_Returns409(t *testing.T) {
	reg := &stubRegistry{addErr: fmt.Errorf("already exists")}
	hs := newTestHTTPServerWithRegistry(reg, "")

	body := `{"id":"ep1","socket":"/tmp/ep1.sock"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/plugins/inference",
		bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	hs.adminRegisterInference(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("expected 409, got %d", w.Code)
	}
}

// ── DELETE /admin/plugins/inference/{id} ─────────────────────────────────────

func TestAdminDeregisterInference_Success(t *testing.T) {
	reg := &stubRegistry{
		endpoints: []plugin.EndpointSummary{{ID: "ep1", Socket: "/tmp/ep1.sock"}},
	}
	hs := newTestHTTPServerWithRegistry(reg, "")

	req := httptest.NewRequest(http.MethodDelete, "/admin/plugins/inference/ep1", nil)
	req.SetPathValue("id", "ep1")
	w := httptest.NewRecorder()
	hs.adminDeregisterInference(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(reg.endpoints) != 0 {
		t.Error("expected endpoint to be removed")
	}
}

func TestAdminDeregisterInference_NotFound_Returns404(t *testing.T) {
	reg := &stubRegistry{}
	hs := newTestHTTPServerWithRegistry(reg, "")

	req := httptest.NewRequest(http.MethodDelete, "/admin/plugins/inference/ghost", nil)
	req.SetPathValue("id", "ghost")
	w := httptest.NewRecorder()
	hs.adminDeregisterInference(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestAdminDeregisterInference_MissingID_Returns400(t *testing.T) {
	reg := &stubRegistry{}
	hs := newTestHTTPServerWithRegistry(reg, "")

	req := httptest.NewRequest(http.MethodDelete, "/admin/plugins/inference/", nil)
	w := httptest.NewRecorder()
	hs.adminDeregisterInference(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// ── auth ──────────────────────────────────────────────────────────────────────

func TestAdminPlugins_AuthRequired(t *testing.T) {
	reg := &stubRegistry{}
	hs := newTestHTTPServerWithRegistry(reg, "secret-token")

	// No token → 401.
	req := httptest.NewRequest(http.MethodGet, "/admin/plugins", nil)
	w := httptest.NewRecorder()
	hs.handleAdminPluginsList(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 without token, got %d", w.Code)
	}

	// Wrong token → 401.
	req = httptest.NewRequest(http.MethodGet, "/admin/plugins", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	w = httptest.NewRecorder()
	hs.handleAdminPluginsList(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with wrong token, got %d", w.Code)
	}

	// Correct token → 200.
	req = httptest.NewRequest(http.MethodGet, "/admin/plugins", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	w = httptest.NewRecorder()
	hs.handleAdminPluginsList(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with correct token, got %d", w.Code)
	}
}
