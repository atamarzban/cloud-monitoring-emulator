package admin

import (
	"encoding/json"
	"net/http"

	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
)

// NewHandler returns an HTTP handler for the admin API.
func NewHandler(s store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/reset", handleReset(s))
	mux.HandleFunc("GET /admin/state", handleState(s))
	return mux
}

func handleReset(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.Reset()
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleState(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state := s.State()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(state)
	}
}
