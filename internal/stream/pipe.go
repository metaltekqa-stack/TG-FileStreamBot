package stream

import (
	"EverythingSuckz/fsb/config"
	"EverythingSuckz/fsb/internal/utils"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/celestix/gotgproto"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
)

// calculateBlockSize func determines optimal block size based on the range requested.
// Smaller ranges use smaller blocks to reduce wasted bandwidth during seeks.
func calculateBlockSize(start, end int64) int64 {
	size := end - start + 1

	switch {
	case size < 512*1024: // < 512KB
		return 64 * 1024 // 64KB blocks
	case size < 4*1024*1024: // < 4MB
		return 256 * 1024 // 256KB blocks
	case size < 32*1024*1024: // < 32MB
		return 512 * 1024 // 512KB blocks
	default:
		return 1024 * 1024 // 1MB blocks by default
	}
}

// StreamPipe reads data from Telegram with concurrent prefetching. implements `io.ReadCloser`.
// credits: [teldrive](https://github.com/tgdrive/teldrive/blob/1071a6e8b5a4076cacc5a0989ef245fae517d837/internal/reader/tg_reader.go)
// Source is one bot's way to a file: the bot, the file's location as that bot sees it, and the
// data centre it is stored in. A stream with several sources shares its blocks between the bots.
type Source struct {
	Client   *gotgproto.Client
	Location tg.InputFileLocationClass
	DC       int
}

type StreamPipe struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    *zap.Logger

	// tg connection
	sources []Source
	turn    *rotation // which bot fetches the next block (rotation.go)

	// range stuff
	start      int64
	end        int64
	blockSize  int64
	totalBytes int64

	// prefetch pipeline
	blockQueue chan []byte

	// current read state
	currentBlock []byte
	blockOffset  int64
	bytesRead    int64

	// lifecycle
	closeOnce sync.Once
}

// class function which creates a StreamPipe with default configuration.
func NewStreamPipe(
	ctx context.Context,
	sources []Source,
	start, end int64,
	log *zap.Logger,
) (io.ReadCloser, error) {

	if start > end {
		return nil, fmt.Errorf("invalid range: start (%d) > end (%d)", start, end)
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no bot to fetch the file with")
	}
	keys := make([]any, len(sources))
	for i, s := range sources {
		keys[i] = s.Client
	}

	ctx, cancel := context.WithCancel(ctx)

	totalBytes := end - start + 1
	blockSize := calculateBlockSize(start, end)

	p := &StreamPipe{
		ctx:        ctx,
		cancel:     cancel,
		log:        log.Named("StreamPipe"),
		sources:    sources,
		turn:       newRotation(keys, botSlowdowns),
		start:      start,
		end:        end,
		blockSize:  blockSize,
		totalBytes: totalBytes,
		blockQueue: make(chan []byte, config.ValueOf.StreamBufferCount),
	}

	// start prefetching in background
	go p.prefetch()

	return p, nil
}

// Read implements io.Reader
func (p *StreamPipe) Read(buf []byte) (n int, err error) {
	if p.bytesRead >= p.totalBytes {
		p.log.Sugar().Debug("EOF (bytesread == contentLength)")
		return 0, io.EOF
	}

	// need a new block?
	if p.blockOffset >= int64(len(p.currentBlock)) {
		select {
		case block, ok := <-p.blockQueue:
			if !ok {
				if p.bytesRead >= p.totalBytes {
					return 0, io.EOF
				}
				return 0, ErrPipeDrained
			}
			p.currentBlock = block
			p.blockOffset = 0
		case <-p.ctx.Done():
			return 0, p.ctx.Err()
		}
	}

	// copy available data
	n = copy(buf, p.currentBlock[p.blockOffset:])
	p.blockOffset += int64(n)
	p.bytesRead += int64(n)

	return n, nil
}

// Close implements io.Closer.
// it cancels prefetching and releases resources.
func (p *StreamPipe) Close() error {
	p.closeOnce.Do(func() {
		p.cancel()
	})
	return nil
}

// prefetch runs in a goroutine: the range's blocks are fetched up to STREAM_CONCURRENCY at once and
// queued for Read in order, each as soon as it is in, small ones first (window.go).
func (p *StreamPipe) prefetch() {
	defer close(p.blockQueue)

	blocks := planBlocks(p.start, p.end, p.blockSize)
	err := fetchInOrder(p.ctx, blocks, config.ValueOf.StreamConcurrency,
		p.downloadBlockWithRetry,
		func(i int, data []byte) error {
			part := trimBlock(blocks[i], data, p.start, p.end)
			if len(part) == 0 {
				return nil
			}
			select {
			case p.blockQueue <- part:
				return nil
			case <-p.ctx.Done():
				return p.ctx.Err()
			}
		})
	// ignore context cancellation cuz it's expected on disconnect
	if err != nil && p.ctx.Err() == nil {
		p.log.Error("block download failed", zap.Error(err))
	}
}

// maxFloodWait is how long one block may wait in all when every bot has been asked to slow down,
// and maxFloodAnswers how many slow down answers it takes before it gives up.
const (
	maxFloodWait    = 60 * time.Second
	maxFloodAnswers = 30
)

// downloadBlockWithRetry fetches a block, each try from the next bot in turn (rotation.go), with
// exponential backoff retry. A bot that Telegram asks to slow down (FLOOD_WAIT,
// FLOOD_PREMIUM_WAIT) is passed over until the time it gave while the other bots carry on, without
// using up a try; only when all have been asked does the block wait, so a stream pauses instead of
// ending. The last try goes over the bot's own connection, in case the connections to the file's
// data centre are the trouble.
func (p *StreamPipe) downloadBlockWithRetry(ctx context.Context, b blockSpec) ([]byte, error) {
	var lastErr error

	// TODO: make configurable later
	backoff := 100 * time.Millisecond   // initial backoff = 100ms
	const maxBackoff = 15 * time.Second // max backoff = 15s
	retries := config.ValueOf.StreamMaxRetries
	var floodWaited time.Duration
	floodAnswers := 0

	for attempt := 0; attempt < retries; {
		// check context before each attempt
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		i, wait := p.turn.pick(time.Now())
		if wait > 0 {
			if floodWaited+wait > maxFloodWait {
				return nil, fmt.Errorf("telegram kept asking every bot to slow down: %w", lastErr)
			}
			floodWaited += wait
			p.log.Info("Every bot was asked to slow down, waiting", zap.Duration("wait", wait), zap.Int64("offset", b.offset))
			select {
			case <-time.After(wait):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		src := p.sources[i]
		api := apiFor(ctx, src.Client, src.DC, p.log)
		if retries > 1 && attempt == retries-1 {
			api = src.Client.API()
		}
		attemptCtx, cancel := context.WithTimeout(ctx, time.Duration(config.ValueOf.StreamTimeoutSec)*time.Second)
		data, err := utils.TimeFuncWithResult(p.log, "downloadBlock", func() ([]byte, error) {
			return p.downloadBlock(attemptCtx, api, src.Location, b)
		})
		cancel()

		if err == nil {
			return data, nil
		}

		lastErr = err

		// don't retry on context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if wait, ok := tgerr.AsFloodWait(err); ok && floodAnswers < maxFloodAnswers {
			floodAnswers++
			botSlowdowns.set(src.Client, time.Now().Add(wait+200*time.Millisecond))
			p.log.Info("Telegram asked a bot to slow down", zap.Int("bot", i+1), zap.Duration("wait", wait), zap.Int64("offset", b.offset))
			continue
		}

		p.log.Warn("Block download failed, retrying", zap.Int("bot", i+1), zap.Int64("offset", b.offset), zap.Int("attempt", attempt+1), zap.Error(err))
		attempt++

		// exponential backoff
		select {
		case <-time.After(backoff):
			backoff *= 2
			// making sure backoff doesn't grow indefinitely in case of persistent failures
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}

// downloadBlock fetches a single block from Telegram.
func (p *StreamPipe) downloadBlock(ctx context.Context, api *tg.Client, location tg.InputFileLocationClass, b blockSpec) ([]byte, error) {
	p.log.Sugar().Debugf("Downloading block at offset %d (block size: %d)", b.offset, b.size)
	res, err := api.UploadGetFile(ctx, &tg.UploadGetFileRequest{
		Offset:   b.offset,
		Limit:    int(b.size),
		Location: location,
	})

	if err != nil {
		return nil, err
	}

	switch result := res.(type) {
	case *tg.UploadFile:
		return result.Bytes, nil
	case *tg.UploadFileCDNRedirect:
		// handle CDN redirect if needed (not implemented here, but could be added)
		// https://core.telegram.org/cdn
		return nil, fmt.Errorf("CDN redirect not supported in this implementation (redirect to DC %d)", result.DCID)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", res)
	}
}
