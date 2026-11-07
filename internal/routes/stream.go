package routes

import (
	"EverythingSuckz/fsb/internal/bot"
	"EverythingSuckz/fsb/internal/stream"
	"EverythingSuckz/fsb/internal/types"
	"EverythingSuckz/fsb/internal/utils"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	range_parser "github.com/quantumsheep/range-parser"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

var log *zap.Logger

func (e *allRoutes) LoadHome(r *Route) {
	log = e.log.Named("Stream")
	defer log.Info("Loaded stream route")
	r.Engine.GET("/stream/:messageID", getStreamRoute)
}

func getStreamRoute(ctx *gin.Context) {
	w := ctx.Writer
	r := ctx.Request

	messageIDParm := ctx.Param("messageID")
	messageID, err := strconv.Atoi(messageIDParm)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	authHash := ctx.Query("hash")
	if authHash == "" {
		http.Error(w, "missing hash param", http.StatusBadRequest)
		return
	}

	worker := bot.GetNextWorker()

	file, err := utils.TimeFuncWithResult(log, "FileFromMessage", func() (*types.File, error) {
		return utils.FileFromMessage(ctx, worker.Client, messageID)
	})
	if err != nil {
		// The bot picked for this request cannot read the file (not an admin of the log channel,
		// say): another bot that can serves it, so one bot set up wrong does not break playback.
		for _, other := range bot.Workers.All() {
			if other == nil || other == worker || other.Client == nil {
				continue
			}
			if f, otherErr := utils.FileFromMessage(ctx, other.Client, messageID); otherErr == nil {
				log.Warn("A bot cannot read the file, so another one serves it: is it an admin of the log channel?",
					zap.String("bot", botName(worker)), zap.Error(err))
				worker, file, err = other, f, nil
				break
			}
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	expectedHash := utils.PackFile(
		file.FileName,
		file.FileSize,
		file.MimeType,
		file.ID,
	)
	if !utils.CheckHash(authHash, expectedHash) {
		http.Error(w, "invalid hash", http.StatusBadRequest)
		return
	}

	// for photo messages
	if file.FileSize == 0 {
		res, err := worker.Client.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: file.Location,
			Offset:   0,
			Limit:    1024 * 1024,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result, ok := res.(*tg.UploadFile)
		if !ok {
			http.Error(w, "unexpected response", http.StatusInternalServerError)
			return
		}
		fileBytes := result.GetBytes()
		ctx.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", file.FileName))
		if r.Method != "HEAD" {
			ctx.Data(http.StatusOK, file.MimeType, fileBytes)
		}
		return
	}

	ctx.Header("Accept-Ranges", "bytes")
	var start, end int64
	rangeHeader := r.Header.Get("Range")

	if rangeHeader == "" {
		start = 0
		end = file.FileSize - 1
		w.WriteHeader(http.StatusOK)
	} else {
		ranges, err := range_parser.Parse(file.FileSize, r.Header.Get("Range"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		ctx.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, file.FileSize))
		log.Info("Content-Range", zap.Int64("start", start), zap.Int64("end", end), zap.Int64("fileSize", file.FileSize))
		w.WriteHeader(http.StatusPartialContent)
	}

	contentLength := end - start + 1
	mimeType := file.MimeType

	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	ctx.Header("Content-Type", mimeType)
	ctx.Header("Content-Length", strconv.FormatInt(contentLength, 10))

	disposition := "inline"

	if ctx.Query("d") == "true" {
		disposition = "attachment"
	}

	ctx.Header("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", disposition, file.FileName))

	if r.Method != "HEAD" {
		pipe, err := stream.NewStreamPipe(ctx, streamSources(ctx, worker, file, messageID), start, end, log)
		if err != nil {
			log.Error("Failed to create stream pipe", zap.Error(err))
			return
		}
		defer pipe.Close()
		if _, err := io.CopyN(w, pipe, contentLength); err != nil {
			if !utils.IsClientDisconnectError(err) {
				log.Error("Error while copying stream", zap.Error(err))
			}
		}
	}
}

// streamSources is every bot that can read the file, the one picked for this request first, so
// the stream's blocks are shared between them (each bot has its own Telegram limits). A bot that
// cannot read the file in time (not an admin of the channel, say) is left out. What each bot
// finds is cached for an hour (FileFromMessage), so this costs Telegram calls on the first
// request for a file only.
func streamSources(ctx context.Context, first *bot.Worker, file *types.File, messageID int) []stream.Source {
	sources := []stream.Source{{Client: first.Client, Location: file.Location, DC: file.DCID}}
	all := bot.Workers.All()
	if len(all) < 2 {
		return sources
	}
	lookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	found := make([]*stream.Source, len(all))
	var wg sync.WaitGroup
	for i, w := range all {
		if w == nil || w.Client == nil || w.Client == first.Client {
			continue
		}
		wg.Add(1)
		go func(i int, w *bot.Worker) {
			defer wg.Done()
			f, err := utils.FileFromMessage(lookCtx, w.Client, messageID)
			if err != nil || f.ID != file.ID {
				log.Warn("A bot cannot read this file, leaving it out of the stream: is it an admin of the log channel?",
					zap.String("bot", botName(w)), zap.Error(err))
				return
			}
			found[i] = &stream.Source{Client: w.Client, Location: f.Location, DC: f.DCID}
		}(i, w)
	}
	wg.Wait()
	for _, s := range found {
		if s != nil {
			sources = append(sources, *s)
		}
	}
	return sources
}

// botName is a bot's @username for the log, or its worker number.
func botName(w *bot.Worker) string {
	if w != nil && w.Self != nil && w.Self.Username != "" {
		return "@" + w.Self.Username
	}
	if w != nil {
		return "worker " + strconv.Itoa(w.ID)
	}
	return "unknown"
}
