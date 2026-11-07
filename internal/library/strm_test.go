package library

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestName(t *testing.T) {
	cases := []struct {
		file, caption string
		want          string
	}{
		{"Some.Title.2024.1080p.WEB-DL.mkv", "", "Some.Title.2024.1080p.WEB-DL"},
		{"Title (2024).mp4", "ignored", "Title (2024)"},
		{"", "Title (2023) Malayalam\nsecond line", "Title (2023) Malayalam"},
		{"", "", "Telegram video 42"},
		{"bad<>:\"/\\|?*name.mkv", "", "bad name"},
		{"   ", "  \n", "Telegram video 42"},
		{"Title...", "", "Title"},
		{"CON.mkv", "", "CON video"},
		{"com1", "", "com1 video"},
		{"notes.txt", "", "notes.txt"},
		{"tab\there.avi", "", "tab here"},
	}
	for _, c := range cases {
		if got := Name(c.file, c.caption, 42); got != c.want {
			t.Errorf("Name(%q, %q) = %q, want %q", c.file, c.caption, got, c.want)
		}
	}
	long := strings.Repeat("ab", 200) + ".mkv"
	if n := len([]rune(Name(long, "", 1))); n > maxNameRunes {
		t.Errorf("long name kept %d characters", n)
	}
	if got := Name(strings.Repeat("é", 300), "", 1); !strings.HasPrefix(got, "éé") || len([]rune(got)) != maxNameRunes {
		t.Errorf("a long accented name was cut wrongly: %d characters", len([]rune(got)))
	}
}

func TestIsVideo(t *testing.T) {
	if !IsVideo("video/x-matroska", "") || !IsVideo("application/octet-stream", "a.MKV") || IsVideo("application/pdf", "a.pdf") || IsVideo("", "") {
		t.Error("IsVideo misjudged a file")
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestAdd(t *testing.T) {
	dir := t.TempDir()
	name, err := Add(dir, 111, "Title (2024)", "http://pc:8080/stream/1?hash=a")
	if err != nil || name != "Title (2024)" {
		t.Fatalf("first add: %q, %v", name, err)
	}
	if got := read(t, filepath.Join(dir, "Title (2024)", "Title (2024).strm")); got != "http://pc:8080/stream/1?hash=a\n" {
		t.Fatalf("strm holds %q", got)
	}
	// The same video again: its own entry, with the new link.
	name, err = Add(dir, 111, "Title (2024)", "http://pc:8080/stream/2?hash=b")
	if err != nil || name != "Title (2024)" {
		t.Fatalf("same video again: %q, %v", name, err)
	}
	if got := read(t, filepath.Join(dir, "Title (2024)", "Title (2024).strm")); !strings.Contains(got, "stream/2") {
		t.Fatalf("link not refreshed: %q", got)
	}
	// Another video with the same name: an entry of its own.
	name, err = Add(dir, 222, "Title (2024)", "http://pc:8080/stream/3?hash=c")
	if err != nil || name != "Title (2024) (2)" {
		t.Fatalf("other video: %q, %v", name, err)
	}
	// A folder the user made themselves is never written into.
	if err := os.Mkdir(filepath.Join(dir, "Mine"), 0o755); err != nil {
		t.Fatal(err)
	}
	name, err = Add(dir, 333, "Mine", "http://pc:8080/stream/4?hash=d")
	if err != nil || name != "Mine (2)" {
		t.Fatalf("user's folder: %q, %v", name, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "Mine", "Mine.strm")); err == nil {
		t.Fatal("wrote into the user's own folder")
	}
	// No temporary files left behind.
	entries, _ := filepath.Glob(filepath.Join(dir, "*", ".fsb-*.tmp"))
	if len(entries) != 0 {
		t.Fatalf("left %v", entries)
	}
}

func TestAddRefusesBadFolders(t *testing.T) {
	if _, err := Add("", 1, "x", "http://a"); err == nil {
		t.Error("no folder accepted")
	}
	if _, err := Add(filepath.Join(t.TempDir(), "missing"), 1, "x", "http://a"); err == nil {
		t.Error("missing folder accepted")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Add(file, 1, "x", "http://a"); err == nil {
		t.Error("a file accepted as the folder")
	}
	if _, err := Add(t.TempDir(), 1, "x", " "); err == nil {
		t.Error("an empty link accepted")
	}
}
