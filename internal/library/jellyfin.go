package library

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Jellyfin tells a Jellyfin server about new library entries, so a video shows up without a
// library scan, and whether or not the server's own folder watching works on that drive.
type Jellyfin struct {
	URL    string // the server as the bot reaches it, http://localhost:8096 on the same PC
	APIKey string // Dashboard, API Keys
	Client *http.Client
}

// Enabled is a server and a key to reach it with.
func (j Jellyfin) Enabled() bool {
	return strings.TrimSpace(j.URL) != "" && strings.TrimSpace(j.APIKey) != ""
}

// Added reports a new entry (its folder) through POST /Library/Media/Updated, as an outside tool
// that adds media does. Jellyfin looks at the folder after its library monitor delay, a minute by
// default, and lists the video.
func (j Jellyfin) Added(path string) error {
	body, err := json.Marshal(map[string][]map[string]string{
		"Updates": {{"Path": path, "UpdateType": "Created"}},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(strings.TrimSpace(j.URL), "/")+"/Library/Media/Updated", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("JELLYFIN_URL %q: %w", j.URL, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", `MediaBrowser Token="`+strings.TrimSpace(j.APIKey)+`"`)
	client := j.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("could not reach Jellyfin at %s: %w", j.URL, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return errors.New("Jellyfin refused the API key (JELLYFIN_API_KEY)")
	default:
		return fmt.Errorf("Jellyfin answered %s", resp.Status)
	}
}
