package stream

import (
	"context"
)

// How a range is fetched from Telegram: as blocks requested several at once (up to
// STREAM_CONCURRENCY in flight), each handed to the player as soon as it and the blocks before it
// are in, rather than in batches that wait for their slowest block. The first blocks of a range are
// small, so a player starting or seeking gets its first bytes after one short request instead of
// after a whole batch of megabytes. Only the standard library here, so it is tested on its own.

const (
	// Telegram's upload.getFile takes a limit that divides 1 MB, at an offset that is a multiple of
	// 4 KB and does not cross a 1 MB boundary. A power of two block at an offset that is a multiple
	// of its own size meets all of that.
	minBlock = 4 << 10
	maxBlock = 1 << 20
)

// rampUp is the largest size of the first blocks of a range, then the range's own block size.
var rampUp = []int64{64 << 10, 128 << 10, 128 << 10, 256 << 10, 256 << 10, 512 << 10, 512 << 10}

type blockSpec struct {
	offset int64
	size   int64
}

// planBlocks lists the blocks that cover start..end (inclusive), none larger than largest (a power
// of two from 4 KB to 1 MB), small ones first.
func planBlocks(start, end, largest int64) []blockSpec {
	if largest < minBlock {
		largest = minBlock
	}
	if largest > maxBlock {
		largest = maxBlock
	}
	for largest&(largest-1) != 0 { // not a power of two: the next one down
		largest &= largest - 1
	}
	if start < 0 {
		start = 0
	}
	var blocks []blockSpec
	offset := start &^ (minBlock - 1)
	for i := 0; offset <= end; i++ {
		size := largest
		if i < len(rampUp) && rampUp[i] < size {
			size = rampUp[i]
		}
		// Aligned to its own size (so it stays inside one megabyte), and no larger than what is
		// left of the range: a short range read near the end of a file fetches a few KB, not 1 MB.
		for size > minBlock && (offset%size != 0 || end-offset+1 <= size/2) {
			size /= 2
		}
		blocks = append(blocks, blockSpec{offset: offset, size: size})
		offset += size
	}
	return blocks
}

// trimBlock is the part of a fetched block that lies inside start..end. Telegram answers short
// at the end of a file, so the data may be shorter than the block.
func trimBlock(b blockSpec, data []byte, start, end int64) []byte {
	lo := start - b.offset
	if lo < 0 {
		lo = 0
	}
	hi := end + 1 - b.offset
	if hi > int64(len(data)) {
		hi = int64(len(data))
	}
	if lo >= hi {
		return nil
	}
	return data[lo:hi]
}

type fetchResult struct {
	data []byte
	err  error
}

// fetchInOrder fetches blocks with up to inFlight requests at once and calls deliver for each in
// order, as soon as it is in. A block is requested only once the one inFlight places before it
// has been delivered, so at most inFlight blocks wait in memory here beside what deliver holds.
// The first failure, or ctx ending, stops it; requests still running are cancelled.
func fetchInOrder(
	ctx context.Context,
	blocks []blockSpec,
	inFlight int,
	fetch func(context.Context, blockSpec) ([]byte, error),
	deliver func(index int, data []byte) error,
) error {
	if inFlight < 1 {
		inFlight = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]chan fetchResult, len(blocks))
	launched := 0
	for i := range blocks {
		for ; launched < len(blocks) && launched < i+inFlight; launched++ {
			ch := make(chan fetchResult, 1)
			results[launched] = ch
			go func(b blockSpec) {
				data, err := fetch(ctx, b)
				ch <- fetchResult{data: data, err: err}
			}(blocks[launched])
		}
		var r fetchResult
		select {
		case r = <-results[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
		results[i] = nil
		if r.err != nil {
			return r.err
		}
		if err := deliver(i, r.data); err != nil {
			return err
		}
	}
	return nil
}
