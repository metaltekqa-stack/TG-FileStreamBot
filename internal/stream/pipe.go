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
type StreamPipe struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    *zap.Logger

	// tg connection
	client   *gotgproto.Client
	api      *tg.Client // straight to the file's data centre where possible (apiFor)
	location tg.InputFileLocationClass

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
	client *gotgproto.Client,
	location tg.InputFileLocationClass,
	dc int,
	start, end int64,
	log *zap.Logger,
) (io.ReadCloser, error) {

	if start > end {
		return nil, fmt.Errorf("invalid range: start (%d) > end (%d)", start, end)
	}

	ctx, cancel := context.WithCancel(ctx)

	totalBytes := end - start + 1
	blockSize := calculateBlockSize(start, end)

	p := &StreamPipe{
		ctx:        ctx,
		cancel:     cancel,
		log:        log.Named("StreamPipe"),
		client:     client,
		api:        apiFor(ctx, client, dc, log),
		location:   location,
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

// downloadBlockWithRetry fetches a block with exponential backoff retry.
func (p *StreamPipe) downloadBlockWithRetry(ctx context.Context, b blockSpec) ([]byte, error) {
	var lastErr error

	// TODO: make configurable later
	backoff := 100 * time.Millisecond   // initial backoff = 100ms
	const maxBackoff = 15 * time.Second // max backoff = 15s

	for attempt := 0; attempt < config.ValueOf.StreamMaxRetries; attempt++ {
		// check context before each attempt
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		attemptCtx, cancel := context.WithTimeout(ctx, time.Duration(config.ValueOf.StreamTimeoutSec)*time.Second)
		data, err := utils.TimeFuncWithResult(p.log, "downloadBlock", func() ([]byte, error) {
			return p.downloadBlock(attemptCtx, b)
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
func (p *StreamPipe) downloadBlock(ctx context.Context, b blockSpec) ([]byte, error) {
	p.log.Sugar().Debugf("Downloading block at offset %d (block size: %d)", b.offset, b.size)
	res, err := p.api.UploadGetFile(ctx, &tg.UploadGetFileRequest{
		Offset:   b.offset,
		Limit:    int(b.size),
		Location: p.location,
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
