package httpserver

import "net/http"

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := s.metrics.WritePrometheus(r.Context(), w); err != nil {
		http.Error(w, "collect metrics failed", http.StatusInternalServerError)
		return
	}
}
