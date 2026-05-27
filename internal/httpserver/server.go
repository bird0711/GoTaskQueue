package httpserver

import (
	"context"
	"net"
	"net/http"

	"github.com/bird0711/GoTaskQueue/internal/metrics"
)

type Config struct {
	Addr string
}

type Server struct {
	httpServer *http.Server
	tasks      TaskStore
	metrics    *metrics.Registry
}

func New(cfg Config, tasks TaskStore, registry *metrics.Registry) *Server {
	mux := http.NewServeMux()
	server := &Server{
		tasks:   tasks,
		metrics: registry,
		httpServer: &http.Server{
			Addr:    cfg.Addr,
			Handler: mux,
		},
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /", server.handleDashboard)
	mux.HandleFunc("GET /dashboard", server.handleDashboard)
	mux.HandleFunc("GET /dashboard/tasks/{id}", server.handleDashboardTaskDetail)
	mux.HandleFunc("POST /tasks", server.handleCreateTask)
	mux.HandleFunc("GET /tasks/{id}", server.handleGetTask)
	if registry != nil {
		mux.HandleFunc("GET /metrics", server.handleMetrics)
	}

	return server
}

func (s *Server) Run() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) RunListener(listener net.Listener) error {
	return s.httpServer.Serve(listener)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
