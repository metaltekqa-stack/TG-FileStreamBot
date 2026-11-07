// Package library puts each video the bot is sent into a media server's library (Jellyfin, Emby
// or Kodi) without downloading it: a .strm file, a text file holding the video's stream link, in
// STRM_DIR/<name>/<name>.strm. The server, watching that folder, adds the video with its poster
// and details, and playing it streams from Telegram through this bot. Nothing of the video itself
// is written to the disk.
package library

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// idFile sits next to each .strm and holds the Telegram document id of the video it links to, so
// the same video sent again refreshes its own entry, while a different video with the same name
// gets an entry of its own. Media servers ignore it.
const idFile = "fsb-id.txt"

// maxNameRunes keeps a folder plus its .strm file well inside Windows' 260 character paths.
const maxNameRunes = 120

var videoExts = map[string]bool{
	".mkv": true, ".mp4": true, ".m4v": true, ".avi": true, ".mov": true, ".webm": true,
	".ts": true, ".m2ts": true, ".mts": true, ".wmv": true, ".flv": true, ".mpg": true,
	".mpeg": true, ".3gp": true, ".ogv": true, ".vob": true, ".divx": true, ".rmvb": true,
}

// IsVideo tells whether a file the bot was sent belongs in the library: a video by its type or,
// for files Telegram knows only as documents, by the extension of their name.
func IsVideo(mimeType, fileName string) bool {
	if strings.HasPrefix(strings.ToLower(mimeType), "video/") {
		return true
	}
	return videoExts[strings.ToLower(filepath.Ext(fileName))]
}

// Name is the library name for a video: its file name without the extension; for a video sent
// without one, the first line of its caption; and failing both, "Telegram video <message id>".
// Whatever Windows does not allow in a name is replaced, so the result is always usable.
func Name(fileName, caption string, messageID int) string {
	base := strings.TrimSpace(fileName)
	if ext := filepath.Ext(base); videoExts[strings.ToLower(ext)] {
		base = strings.TrimSuffix(base, ext)
	}
	name := clean(base)
	if name == "" {
		first, _, _ := strings.Cut(caption, "\n")
		name = clean(first)
	}
	if name == "" {
		name = "Telegram video " + strconv.Itoa(messageID)
	}
	return name
}

// clean makes s safe as a folder and file name on Windows, macOS and Linux.
func clean(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == utf8.RuneError, unicode.IsControl(r), strings.ContainsRune(`<>:"/\|?*`, r):
			b.WriteRune(' ')
		default:
			b.WriteRune(r)
		}
	}
	name := strings.Join(strings.Fields(b.String()), " ")
	if utf8.RuneCountInString(name) > maxNameRunes {
		name = string([]rune(name)[:maxNameRunes])
	}
	// Windows drops a name's trailing dots and spaces, so a folder "Title." would not be found again.
	name = strings.TrimRight(name, ". ")
	if reserved(name) {
		name += " video"
	}
	return name
}

// reserved is a name Windows keeps for devices (CON, NUL, COM1 and the like), with or without an extension.
func reserved(name string) bool {
	stem, _, _ := strings.Cut(strings.ToUpper(name), ".")
	switch stem {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	if len(stem) == 4 && (strings.HasPrefix(stem, "COM") || strings.HasPrefix(stem, "LPT")) {
		return stem[3] >= '1' && stem[3] <= '9'
	}
	return false
}

// Add writes the library entry for a video and returns the name it is listed under. The same
// video (by its Telegram document id) sent again updates its entry with the new link; another
// video that would get the same name is listed as "<name> (2)", "<name> (3)" and so on.
func Add(dir string, documentID int64, name, link string) (string, error) {
	if strings.TrimSpace(dir) == "" {
		return "", errors.New("no library folder set (STRM_DIR)")
	}
	if strings.TrimSpace(link) == "" {
		return "", errors.New("no link to write")
	}
	info, err := os.Stat(dir)
	if err != nil {
		return "", fmt.Errorf("library folder %s: %w", dir, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("library folder %s is a file, not a folder", dir)
	}
	id := strconv.FormatInt(documentID, 10)
	for n := 1; n <= 100; n++ {
		listed := name
		if n > 1 {
			listed = fmt.Sprintf("%s (%d)", name, n)
		}
		folder := filepath.Join(dir, listed)
		owner, err := os.ReadFile(filepath.Join(folder, idFile))
		switch {
		case err == nil && strings.TrimSpace(string(owner)) != id:
			continue // another video's entry
		case err != nil && !errors.Is(err, os.ErrNotExist):
			return "", fmt.Errorf("reading %s: %w", folder, err)
		case err != nil:
			// No id file: a free name, unless a folder of the user's own is already there.
			if _, statErr := os.Stat(folder); statErr == nil {
				continue
			}
		}
		if err := os.MkdirAll(folder, 0o755); err != nil {
			return "", fmt.Errorf("creating %s: %w", folder, err)
		}
		if err := writeFile(filepath.Join(folder, idFile), id+"\n"); err != nil {
			return "", err
		}
		if err := writeFile(filepath.Join(folder, listed+".strm"), link+"\n"); err != nil {
			return "", err
		}
		return listed, nil
	}
	return "", fmt.Errorf("more than 100 videos are already listed as %q", name)
}

// writeFile writes through a temporary file and a rename, so a media server watching the folder
// never reads a half written link.
func writeFile(path, content string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".fsb-*.tmp")
	if err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	_, writeErr := tmp.WriteString(content)
	closeErr := tmp.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(tmp.Name(), path)
	}
	if writeErr != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("writing %s: %w", path, writeErr)
	}
	return nil
}
