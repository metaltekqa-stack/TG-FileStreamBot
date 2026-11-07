package stream

import (
	"context"
	"sync"
	"time"

	"EverythingSuckz/fsb/config"

	"github.com/celestix/gotgproto"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"
)

// Connections straight to the data centre a file is stored in. Through the bot's own connection, a
// block of a file kept in another data centre went to the bot's data centre first, came back as
// FILE_MIGRATE and was sent again over a single connection to the file's one, so every block paid
// an extra round trip and all blocks of every stream queued on that one connection. These pools
// hold up to STREAM_CONCURRENCY (at most 8) connections to each data centre for each bot, opened (and, away
// from the bot's own data centre, authorised) once and kept for the bot's life.

type poolKey struct {
	client *gotgproto.Client
	dc     int
}

var (
	poolsMu sync.Mutex
	pools   = map[poolKey]*tg.Client{}
)

// apiFor is the API to fetch a file stored in data centre dc with: a pool of connections to it,
// or the bot's own connection when dc is not known or the pool cannot be opened.
func apiFor(ctx context.Context, client *gotgproto.Client, dc int, log *zap.Logger) *tg.Client {
	if dc <= 0 {
		return client.API()
	}
	key := poolKey{client: client, dc: dc}
	poolsMu.Lock()
	defer poolsMu.Unlock()
	if api, ok := pools[key]; ok {
		return api
	}
	// Several requests share a connection, so more than 8 connections a data centre buys little and
	// only looks like a flood to Telegram.
	size := int64(min(max(1, config.ValueOf.StreamConcurrency), 8))
	openCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var invoker telegram.CloseInvoker
	var err error
	if dc == client.Client.Config().ThisDC {
		invoker, err = client.Client.Pool(size)
	} else {
		invoker, err = client.Client.DC(openCtx, dc, size)
	}
	if err != nil {
		// Not kept: the next stream tries again.
		log.Warn("Could not open connections to the file's data centre, using the bot's own", zap.Int("dc", dc), zap.Error(err))
		return client.API()
	}
	log.Info("Opened connections to a data centre", zap.Int("dc", dc), zap.Int64("connections", size))
	api := tg.NewClient(invoker)
	pools[key] = api
	return api
}
