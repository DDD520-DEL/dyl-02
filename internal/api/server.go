package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Routes registers every HTTP endpoint on the mux.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/tasks", s.handleSubmit)
	mux.HandleFunc("POST /api/v1/tasks/batch", s.handleSubmitBatch)
	mux.HandleFunc("GET /api/v1/tasks/{id}", s.handleGet)
	mux.HandleFunc("GET /api/v1/tasks/{id}/history", s.handleHistory)
	mux.HandleFunc("POST /api/v1/tasks/{id}/cancel", s.handleCancel)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /healthz/ready", s.handleReady)
	mux.HandleFunc("GET /healthz/history", s.handleHealthHistory)
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var in TaskInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	t, err := s.Submit(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (s *Server) handleSubmitBatch(w http.ResponseWriter, r *http.Request) {
	var inputs []TaskInput
	if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	tasks, err := s.SubmitBatch(r.Context(), inputs)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, tasks)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	t, err := s.Get(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if err := s.Cancel(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	hist, err := s.History(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, hist)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if s.health == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
		return
	}
	writeJSON(w, http.StatusOK, s.health.Check(r.Context()))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.health == nil || !s.health.Ready(r.Context()) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleHealthHistory(w http.ResponseWriter, r *http.Request) {
	if s.health == nil {
		writeJSON(w, http.StatusOK, map[string]any{"recent": []any{}, "summary": map[string]any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"recent":  s.health.Recent(10),
		"summary": s.health.Summary(),
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// Serve runs the HTTP server until the context is cancelled.
func (s *Server) Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := &http.Server{Addr: addr, Handler: withLogging(mux)}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	return srv.ListenAndServe()
}
