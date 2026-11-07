package library

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJellyfinAdded(t *testing.T) {
	var gotPath, gotAuth, gotType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/Library/Media/Updated" {
			http.NotFound(w, r)
			return
		}
		gotAuth = r.Header.Get("Authorization")
		gotType = r.Header.Get("Content-Type")
		var body struct {
			Updates []struct{ Path, UpdateType string }
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Updates) != 1 {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		gotPath = body.Updates[0].Path
		if r.Header.Get("Authorization") != `MediaBrowser Token="key"` {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	j := Jellyfin{URL: srv.URL + "/", APIKey: " key ", Client: srv.Client()}
	if !j.Enabled() {
		t.Fatal("not enabled with a URL and a key")
	}
	if err := j.Added(`D:\Media\movies\Title (2024)`); err != nil {
		t.Fatal(err)
	}
	if gotPath != `D:\Media\movies\Title (2024)` || gotAuth != `MediaBrowser Token="key"` || gotType != "application/json" {
		t.Fatalf("sent path %q, auth %q, type %q", gotPath, gotAuth, gotType)
	}

	bad := Jellyfin{URL: srv.URL, APIKey: "wrong", Client: srv.Client()}
	if err := bad.Added("x"); err == nil || !strings.Contains(err.Error(), "API key") {
		t.Fatalf("a refused key gave %v", err)
	}
	gone := Jellyfin{URL: "http://127.0.0.1:1", APIKey: "key"}
	if err := gone.Added("x"); err == nil || !strings.Contains(err.Error(), "could not reach") {
		t.Fatalf("an unreachable server gave %v", err)
	}
	if (Jellyfin{URL: "http://a"}).Enabled() || (Jellyfin{APIKey: "k"}).Enabled() {
		t.Fatal("enabled without both a URL and a key")
	}
}
