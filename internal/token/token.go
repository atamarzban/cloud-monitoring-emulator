package token

import (
	"encoding/json"
	"net/http"
)

// NewHandler returns an HTTP handler for the fake OAuth2 token endpoint.
func NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", handleToken)
	return mux
}

func handleToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}

	grantType := r.FormValue("grant_type")
	if grantType != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "unsupported_grant_type",
			"error_description": "Only urn:ietf:params:oauth:grant-type:jwt-bearer is supported",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token": "emulator-fake-token",
		"token_type":   "Bearer",
		"expires_in":   3600,
	})
}
