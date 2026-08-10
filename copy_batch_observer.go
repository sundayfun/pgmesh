package pgmesh

import (
	"context"
	"time"
)

// CopyBatchFlushReason identifies why a physical COPY batch became ready.
type CopyBatchFlushReason string

// Physical COPY batch flush reasons.
const (
	CopyBatchFlushReasonFull      CopyBatchFlushReason = "full"
	CopyBatchFlushReasonLinger    CopyBatchFlushReason = "linger"
	CopyBatchFlushReasonManual    CopyBatchFlushReason = "manual"
	CopyBatchFlushReasonUnbatched CopyBatchFlushReason = "unbatched"
)

// CopyBatchObservation describes one completed physical COPY operation.
type CopyBatchObservation struct {
	RowCount          int
	SubmissionCount   int
	Reason            CopyBatchFlushReason
	QueueDuration     time.Duration
	ExecutionDuration time.Duration
	Err               error
}

// CopyBatchObserver receives completed physical COPY observations. A batcher
// may invoke an observer concurrently.
type CopyBatchObserver func(ctx context.Context, observation CopyBatchObservation)
