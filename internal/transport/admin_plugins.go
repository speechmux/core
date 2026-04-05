package transport

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/speechmux/core/internal/plugin"
)

// PluginRegistry supports runtime registration and deregistration of inference
// plugin endpoints. Implemented by *runtime.Application.
type PluginRegistry interface {
	AddInferenceEndpoint(id, socket string) error
	RemoveInferenceEndpoint(id string) error
	ListInferenceEndpoints() []plugin.EndpointSummary
}

// handleAdminPluginsList handles GET /admin/plugins.
// Returns a JSON object listing all registered inference endpoints.
func (h *HTTPServer) handleAdminPluginsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.authorizeAdmin(w, r) {
		return
	}
	if h.registry == nil {
		http.Error(w, `{"code":"ERR4001","message":"plugin registry disabled"}`,
			http.StatusNotImplemented)
		return
	}

	type response struct {
		Inference []plugin.EndpointSummary `json:"inference"`
	}
	resp := response{Inference: h.registry.ListInferenceEndpoints()}
	if resp.Inference == nil {
		resp.Inference = []plugin.EndpointSummary{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleAdminPluginsInference handles POST and DELETE on /admin/plugins/inference
// and DELETE /admin/plugins/inference/{id}.
//
//	POST   /admin/plugins/inference       — register a new endpoint
//	DELETE /admin/plugins/inference/{id}  — remove an existing endpoint
func (h *HTTPServer) handleAdminPluginsInference(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeAdmin(w, r) {
		return
	}
	if h.registry == nil {
		http.Error(w, `{"code":"ERR4001","message":"plugin registry disabled"}`,
			http.StatusNotImplemented)
		return
	}

	switch r.Method {
	case http.MethodPost:
		h.adminRegisterInference(w, r)
	case http.MethodDelete:
		h.adminDeregisterInference(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type registerRequest struct {
	ID     string `json:"id"`
	Socket string `json:"socket"`
}

func (h *HTTPServer) adminRegisterInference(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"code":"ERR1001","message":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.Socket == "" {
		http.Error(w, `{"code":"ERR1001","message":"id and socket are required"}`, http.StatusBadRequest)
		return
	}

	if err := h.registry.AddInferenceEndpoint(req.ID, req.Socket); err != nil {
		http.Error(w, jsonError("ERR3001", err.Error()), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "registered",
		"id":     req.ID,
		"socket": req.Socket,
	})
}

func (h *HTTPServer) adminDeregisterInference(w http.ResponseWriter, r *http.Request) {
	// Extract id from path: /admin/plugins/inference/{id}
	// Go 1.22+ ServeMux supports {id} wildcards; fall back to string trimming.
	id := r.PathValue("id")
	if id == "" {
		// Fallback for paths without wildcard registration.
		id = strings.TrimPrefix(r.URL.Path, "/admin/plugins/inference/")
	}
	if id == "" {
		http.Error(w, `{"code":"ERR1001","message":"endpoint id required in path"}`, http.StatusBadRequest)
		return
	}

	if err := h.registry.RemoveInferenceEndpoint(id); err != nil {
		http.Error(w, jsonError("ERR3002", err.Error()), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "removed",
		"id":     id,
	})
}

// authorizeAdmin checks the Authorization header when adminToken is set.
// Returns true when the request is authorized; writes an error response and
// returns false otherwise.
func (h *HTTPServer) authorizeAdmin(w http.ResponseWriter, r *http.Request) bool {
	if h.adminToken == "" {
		return true
	}
	if r.Header.Get("Authorization") != "Bearer "+h.adminToken {
		http.Error(w, `{"code":"ERR4004","message":"admin token invalid"}`, http.StatusUnauthorized)
		return false
	}
	return true
}

func jsonError(code, msg string) string {
	b, _ := json.Marshal(map[string]string{"code": code, "message": msg})
	return string(b)
}
