package pgmesh

import (
	"errors"
	"time"
)

// DefaultCopyBatchLinger is the default coalescing window for a partial batch.
const DefaultCopyBatchLinger = time.Millisecond

// DefaultCopyBatchMaxConcurrentBatches is the default execution limit per
// database target.
const DefaultCopyBatchMaxConcurrentBatches = 32

// CopyBatchConfig controls how concurrent submissions are coalesced into
// physical COPY operations.
type CopyBatchConfig struct {
	// MaxRowsPerBatch is the maximum number of rows in one physical COPY. Zero
	// leaves batches unbounded by row count.
	MaxRowsPerBatch int

	// Linger is how long a partial batch remains open for coalescing after its
	// first row is accepted. Full and manually flushed batches become ready
	// sooner. Zero uses DefaultCopyBatchLinger.
	Linger time.Duration

	// MaxConcurrentBatches is the maximum number of physical COPY batches in
	// flight for one database target. Queued batches do not count toward the
	// limit. Zero uses
	// DefaultCopyBatchMaxConcurrentBatches.
	MaxConcurrentBatches int
}

// Validate validates the configuration without creating a batcher.
func (c CopyBatchConfig) Validate() error {
	if c.MaxRowsPerBatch < 0 {
		return errors.New("pgmesh: copy batch maximum rows per batch must not be negative")
	}
	if c.Linger < 0 {
		return errors.New("pgmesh: copy batch linger must not be negative")
	}
	if c.MaxConcurrentBatches < 0 {
		return errors.New("pgmesh: copy batch maximum concurrent batches must not be negative")
	}
	return nil
}

func (c CopyBatchConfig) normalized() CopyBatchConfig {
	if c.Linger == 0 {
		c.Linger = DefaultCopyBatchLinger
	}
	if c.MaxConcurrentBatches == 0 {
		c.MaxConcurrentBatches = DefaultCopyBatchMaxConcurrentBatches
	}
	return c
}
