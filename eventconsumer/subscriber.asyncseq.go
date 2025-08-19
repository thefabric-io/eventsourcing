package eventconsumer

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thefabric-io/eventsourcing"
	"github.com/thefabric-io/transactional"
)

// AsyncSequentialSubscriber processes events sequentially (in order) but does not hold a transaction open during event processing.
type AsyncSequentialSubscriber[S eventsourcing.AggregateState] struct {
	transactional    transactional.Transactional
	name             string
	consumerStore    ConsumerStore
	eventStore       eventsourcing.EventStore[S]
	handlers         map[string]Handler[S]
	handlersLock     sync.RWMutex
	reservationMu    sync.Mutex
	processedCount   int64
	batchSize        int
	waitTime         time.Duration
	waitTimeIfEvents time.Duration
	// handlerTimeout bounds per-event handler execution; if 0, no per-event timeout
	handlerTimeout  time.Duration
	queue           chan *eventsourcing.Event[S]
	ackQueue        chan int
	ackBatchSize    int
	ackBatchTimeout time.Duration
	wg              sync.WaitGroup
	closeQueueOnce  sync.Once
	// rng is only used from the Start() goroutine via enqueueEventWithBackoff; not accessed concurrently
	rng               *rand.Rand
	ctx               context.Context
	cancel            context.CancelFunc
	reservationActive bool
	reservedEnd       int
	// backoff state
	retryInitial    time.Duration
	retryMax        time.Duration
	retryMultiplier float64
	// per-aggregate scheduler state
	aggMu     sync.Mutex
	aggQueues map[string]*aggregateQueue[S]
}

type aggregateQueue[S eventsourcing.AggregateState] struct {
	events       []*eventsourcing.Event[S]
	nextReadyAt  time.Time
	attemptsByOf map[int]int
}

// AsyncSequentialSubscriberOption allows configuring optional parameters for
// AsyncSequentialSubscriber without a long positional-args constructor.
//
// Example:
//
//	s := NewAsyncSequentialSubscriber(ctx, NewAsyncSequentialSubscriberParams[S]{
//		Tx:            tx,
//		Name:          "name",
//		ConsumerStore: consumerStore,
//		EventStore:    eventStore,
//	},
//		WithBatchSize[S](100),
//		WithWaitTimes[S](time.Second, 200*time.Millisecond),
//		WithAckBatch[S](100, time.Second),
//		WithHandlerTimeout[S](2*time.Second),
//		WithRetryPolicy[S](100*time.Millisecond, 5*time.Second, 2.0),
//	)
type AsyncSequentialSubscriberOption[S eventsourcing.AggregateState] func(*AsyncSequentialSubscriber[S])

// WithBatchSize sets the per-iteration fetch size.
func WithBatchSize[S eventsourcing.AggregateState](n int) AsyncSequentialSubscriberOption[S] {
	return func(s *AsyncSequentialSubscriber[S]) {
		if n > 0 {
			s.batchSize = n
		}
	}
}

// NewAsyncSequentialSubscriberParams is a parameter object for configuring
// AsyncSequentialSubscriber in a discoverable and extensible way.
type NewAsyncSequentialSubscriberParams[S eventsourcing.AggregateState] struct {
	Tx            transactional.Transactional
	Name          string
	ConsumerStore ConsumerStore
	EventStore    eventsourcing.EventStore[S]
}

// NewAsyncSequentialParams constructs NewAsyncSequentialSubscriberParams while
// allowing the Go compiler to infer S from the provided eventStore.
// This enables calling NewAsyncSequentialSubscriber without explicit [S].
func NewAsyncSequentialParams[S eventsourcing.AggregateState](
	tx transactional.Transactional,
	name string,
	consumerStore ConsumerStore,
	eventStore eventsourcing.EventStore[S],
) NewAsyncSequentialSubscriberParams[S] {
	return NewAsyncSequentialSubscriberParams[S]{
		Tx:            tx,
		Name:          name,
		ConsumerStore: consumerStore,
		EventStore:    eventStore,
	}
}

// WithWaitTimes sets the idle wait and the wait between batches when events were found.
func WithWaitTimes[S eventsourcing.AggregateState](wait, waitIfEvents time.Duration) AsyncSequentialSubscriberOption[S] {
	return func(s *AsyncSequentialSubscriber[S]) {
		if wait < 0 {
			wait = 0
		}
		if waitIfEvents < 0 {
			waitIfEvents = 0
		}
		s.waitTime = wait
		s.waitTimeIfEvents = waitIfEvents
	}
}

// WithAckBatch configures ack batching behavior.
func WithAckBatch[S eventsourcing.AggregateState](size int, timeout time.Duration) AsyncSequentialSubscriberOption[S] {
	return func(s *AsyncSequentialSubscriber[S]) {
		if size > 0 {
			s.ackBatchSize = size
		}
		if timeout < 0 {
			timeout = 0
		}
		s.ackBatchTimeout = timeout
	}
}

// WithHandlerTimeout sets a per-event handler timeout; set 0 to disable.
func WithHandlerTimeout[S eventsourcing.AggregateState](d time.Duration) AsyncSequentialSubscriberOption[S] {
	return func(s *AsyncSequentialSubscriber[S]) {
		if d < 0 {
			d = 0
		}
		s.handlerTimeout = d
	}
}

// WithRetryPolicy customizes the retry backoff used when a handler returns an error.
// Pass non-positive values to disable backoff.
func WithRetryPolicy[S eventsourcing.AggregateState](initial, max time.Duration, multiplier float64) AsyncSequentialSubscriberOption[S] {
	return func(s *AsyncSequentialSubscriber[S]) {
		if initial <= 0 || max <= 0 || multiplier <= 1 {
			s.retryInitial = 0
			s.retryMax = 0
			s.retryMultiplier = 0
			return
		}
		s.retryInitial = initial
		s.retryMax = max
		s.retryMultiplier = multiplier
	}
}

// sleepCtx sleeps for d or returns early if ctx is cancelled.
// Returns true if the timer fired, false if the context was cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// isSerializableErr returns true if err looks like a PostgreSQL serialization error.
func isSerializableErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "could not serialize") || strings.Contains(s, "SQLSTATE 40001")
}

// NewAsyncSequentialSubscriber creates a new AsyncSequentialSubscriber using a
// required params struct and functional options for tunables.
func NewAsyncSequentialSubscriber[S eventsourcing.AggregateState](ctx context.Context, p NewAsyncSequentialSubscriberParams[S], opts ...AsyncSequentialSubscriberOption[S]) *AsyncSequentialSubscriber[S] {
	return NewAsyncSequentialSubscriberWithOptions[S](ctx, p, opts...)
}

// NewAsyncSequentialSubscriberWithOptions creates a new AsyncSequentialSubscriber
// configured via functional options. Only the required collaborators are
// positional; all tunables are provided via options with sensible defaults.
func NewAsyncSequentialSubscriberWithOptions[S eventsourcing.AggregateState](ctx context.Context, p NewAsyncSequentialSubscriberParams[S], opts ...AsyncSequentialSubscriberOption[S]) *AsyncSequentialSubscriber[S] {
	baseCtx, cancel := context.WithCancel(ctx)

	// Resolve ConsumerStore: param wins; fallback to default provider if set
	store := p.ConsumerStore
	if store == nil && DefaultConsumerStoreProvider != nil {
		store = DefaultConsumerStoreProvider()
	}
	if store == nil {
		panic("eventconsumer: ConsumerStore is nil; provide via params or import pgeventconsumer (or use its helper) to register a default store")
	}

	s := &AsyncSequentialSubscriber[S]{
		transactional: p.Tx,
		name:          p.Name,
		consumerStore: store,
		eventStore:    p.EventStore,
		handlers:      make(map[string]Handler[S]),
		// Defaults
		batchSize:        1,
		waitTime:         0,
		waitTimeIfEvents: 0,
		handlerTimeout:   0,
		// Retry backoff defaults (disabled by default to preserve current behavior)
		retryInitial:    0,
		retryMax:        0,
		retryMultiplier: 0,
		rng:             rand.New(rand.NewSource(seedFor(p.Name))),
		ctx:             baseCtx,
		cancel:          cancel,
		aggQueues:       make(map[string]*aggregateQueue[S]),
	}

	// Apply options
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}

	// Normalize and finalize dependent fields
	if s.batchSize <= 0 {
		s.batchSize = 1
	}
	if s.ackBatchSize <= 0 {
		s.ackBatchSize = 1
	}
	if s.waitTime < 0 {
		s.waitTime = 0
	}
	if s.waitTimeIfEvents < 0 {
		s.waitTimeIfEvents = 0
	}
	if s.ackBatchTimeout < 0 {
		s.ackBatchTimeout = 0
	}

	// Increase channel capacities to reduce back-pressure during spikes.
	// Using a multiple of the batch size allows buffering several batches worth of work.
	queueCapacity := s.batchSize * 4
	if queueCapacity < 1 {
		queueCapacity = 1
	}
	ackQueueCapacity := s.ackBatchSize * 4
	if ackQueueCapacity < 1 {
		ackQueueCapacity = 1
	}
	s.queue = make(chan *eventsourcing.Event[S], queueCapacity)
	s.ackQueue = make(chan int, ackQueueCapacity)

	s.wg.Add(2)
	go s.processLoop()
	go s.ackLoop()
	return s
}

// NewAsyncSequentialSubscriberFromParams mirrors the FIFO style and provides a
// single-argument constructor using a configuration struct.
func NewAsyncSequentialSubscriberFromParams[S eventsourcing.AggregateState](ctx context.Context, p NewAsyncSequentialSubscriberParams[S], opts ...AsyncSequentialSubscriberOption[S]) *AsyncSequentialSubscriber[S] {
	return NewAsyncSequentialSubscriberWithOptions[S](ctx, p, opts...)
}

// closeInputQueue safely closes the input queue once.
func (s *AsyncSequentialSubscriber[S]) closeInputQueue() {
	s.closeQueueOnce.Do(func() { close(s.queue) })
}

// seedFor derives a non-cryptographic per-consumer seed using the consumer name and current time.
func seedFor(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return time.Now().UnixNano() ^ int64(h.Sum64())
}

// Start begins the reservation and processing loop for this consumer instance.
// The provided context controls the lifecycle of this Start invocation. On cancellation, Close() is invoked
// to stop internal goroutines and the method returns promptly with the context error.
func (s *AsyncSequentialSubscriber[S]) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			// Trigger shutdown for internal goroutines
			s.cancel()
			s.closeInputQueue()
			// Wait for processing and acknowledgment loops to finish
			s.wg.Wait()
			return ctx.Err()
		case <-s.ctx.Done():
			// Close() was invoked externally; wait for background loops to finish
			s.wg.Wait()
			return s.ctx.Err()
		default:
			// Respect an existing reservation; delay until acknowledgments catch up
			s.reservationMu.Lock()
			reservationActive := s.reservationActive
			s.reservationMu.Unlock()
			if reservationActive {
				if !sleepCtx(s.ctx, s.waitTimeIfEvents) {
					return s.ctx.Err()
				}
				continue
			}

			// Single short transaction: load consumer, fetch events, reserve
			tx, err := s.transactional.BeginTransaction(s.ctx, transactional.BeginTransactionOptions{
				AccessMode:     transactional.ReadWrite,
				IsolationLevel: transactional.ReadCommitted,
				DeferrableMode: transactional.NotDeferrable,
			})
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"consumer": s.name,
				}).WithError(err).Error("begin transaction failed")
				if !sleepCtx(s.ctx, s.waitTime) {
					return s.ctx.Err()
				}
				continue
			}

			consumer, err := s.consumerStore.Load(s.ctx, tx, s.name)
			if err != nil {
				_ = tx.Rollback()
				logrus.WithFields(logrus.Fields{
					"consumer": s.name,
				}).WithError(err).Error("load consumer state failed")
				if !sleepCtx(s.ctx, s.waitTime) {
					return s.ctx.Err()
				}
				continue
			}

			events, err := s.eventStore.Events(s.ctx, tx, eventsourcing.EventsParams{
				AfterOffset: consumer.OffsetConsumed(),
				TypesOnly:   s.eventTypesProcessing(),
				Limit:       s.batchSize,
			})
			if err != nil {
				_ = tx.Rollback()
				logrus.WithFields(logrus.Fields{
					"consumer": s.name,
				}).WithError(err).Error("fetch events failed")
				if !sleepCtx(s.ctx, s.waitTime) {
					return s.ctx.Err()
				}
				continue
			}

			var last int
			if len(events) > 0 {
				last = events[len(events)-1].Offset()
				if err := s.consumerStore.Save(s.ctx, tx, s.name, consumer.OffsetAcked(), last); err != nil {
					_ = tx.Rollback()
					logrus.WithFields(logrus.Fields{
						"consumer": s.name,
					}).WithError(err).Error("reserve batch failed (advance consumed)")
					if !sleepCtx(s.ctx, s.waitTime) {
						return s.ctx.Err()
					}
					continue
				}
			}

			if err := tx.Commit(); err != nil {
				logrus.WithFields(logrus.Fields{
					"consumer": s.name,
				}).WithError(err).Error("commit transaction failed")
				if !sleepCtx(s.ctx, s.waitTime) {
					return s.ctx.Err()
				}
				continue
			}

			if len(events) == 0 {
				if !sleepCtx(s.ctx, s.waitTime) {
					return s.ctx.Err()
				}
				continue
			}

			// mark reservation active until acks catch up
			s.reservationMu.Lock()
			s.reservationActive = true
			s.reservedEnd = last
			s.reservationMu.Unlock()

			for _, event := range events {
				s.enqueueEventWithBackoff(event)
			}

			if !sleepCtx(s.ctx, s.waitTimeIfEvents) {
				return s.ctx.Err()
			}
		}
	}
}

// enqueueEventWithBackoff enqueues an event without blocking the producer.
// If the queue is full, it retries with exponential backoff (with jitter) up to a cap.
// It aborts early if the subscriber context is canceled.
func (s *AsyncSequentialSubscriber[S]) enqueueEventWithBackoff(event *eventsourcing.Event[S]) {
	const (
		initialBackoff = 10 * time.Millisecond
		maxBackoff     = 500 * time.Millisecond
	)

	backoff := initialBackoff
	for {
		select {
		case s.queue <- event:
			return
		case <-s.ctx.Done():
			return
		default:
		}

		// Add a small jitter to reduce herding and use context-aware sleep
		jitter := time.Duration(s.rng.Int63n(int64(backoff / 4)))
		if !sleepCtx(s.ctx, backoff+jitter) {
			return
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// tryAck attempts to enqueue an offset to the acknowledgment queue.
// If the context is already canceled, it allows a brief grace period to flush.
func (s *AsyncSequentialSubscriber[S]) tryAck(offset int) {
	select {
	case s.ackQueue <- offset:
		return
	case <-s.ctx.Done():
		// short grace period to flush acks
		t := time.NewTimer(50 * time.Millisecond)
		defer t.Stop()
		select {
		case s.ackQueue <- offset:
			return
		case <-t.C:
			return
		}
	}
}

func (s *AsyncSequentialSubscriber[S]) aggregateKey(ev *eventsourcing.Event[S]) string {
	agg := ev.Aggregate()
	if agg != nil && agg.ID() != "" {
		return agg.ID()
	}
	// fallback to isolate by unique offset if no aggregate id (prevents cross-aggregate blocking)
	return fmt.Sprintf("_noagg:%d", ev.Offset())
}

// processLoop schedules events in order per aggregate and skips aggregates that are backoff-delayed.
func (s *AsyncSequentialSubscriber[S]) processLoop() {
	defer s.wg.Done()

	queueClosed := false
	for {
		// Try to pull new events into per-aggregate queues
		pulled := false
		select {
		case ev, ok := <-s.queue:
			pulled = true
			if !ok {
				queueClosed = true
				break
			}
			aggID := s.aggregateKey(ev)
			s.aggMu.Lock()
			q := s.aggQueues[aggID]
			if q == nil {
				q = &aggregateQueue[S]{events: make([]*eventsourcing.Event[S], 0, 8), attemptsByOf: make(map[int]int)}
				s.aggQueues[aggID] = q
			}
			q.events = append(q.events, ev)
			s.aggMu.Unlock()
		default:
		}

		// Process one ready aggregate head if any
		processedOne := false
		now := time.Now()
		s.aggMu.Lock()
		for aggID, q := range s.aggQueues {
			if len(q.events) == 0 {
				continue
			}
			if !q.nextReadyAt.IsZero() && now.Before(q.nextReadyAt) {
				continue
			}
			// Pop head but keep reference until success
			ev := q.events[0]
			s.aggMu.Unlock()

			// Handle outside lock
			logrus.WithFields(logrus.Fields{
				"consumer":  s.name,
				"offset":    ev.Offset(),
				"eventType": ev.Type(),
				"aggregate": aggID,
			}).Debug("event processing started")

			handler := s.getHandler(ev.Type())
			if handler == nil {
				logrus.WithFields(logrus.Fields{
					"consumer":  s.name,
					"offset":    ev.Offset(),
					"eventType": ev.Type(),
				}).Warn("no handler registered; acknowledging event")
				s.tryAck(ev.Offset())
				// Remove from queue
				s.aggMu.Lock()
				q := s.aggQueues[aggID]
				if q != nil && len(q.events) > 0 && q.events[0] == ev {
					q.events = q.events[1:]
				}
				s.aggMu.Unlock()
				processedOne = true
				break
			}

			ctxToUse := s.ctx
			if s.handlerTimeout > 0 {
				var cancel context.CancelFunc
				ctxToUse, cancel = context.WithTimeout(s.ctx, s.handlerTimeout)
				defer cancel()
			}

			var handlerErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						handlerErr = fmt.Errorf("panic: %v", r)
					}
				}()
				handlerErr = handler.HandleEvent(ctxToUse, ev)
			}()

			if handlerErr != nil {
				logrus.WithFields(logrus.Fields{
					"consumer":  s.name,
					"offset":    ev.Offset(),
					"aggregate": aggID,
				}).WithError(handlerErr).Error("event handler failed; scheduling retry")
				// Increment attempt and set nextReadyAt
				s.aggMu.Lock()
				q := s.aggQueues[aggID]
				if q != nil {
					attempt := q.attemptsByOf[ev.Offset()] + 1
					q.attemptsByOf[ev.Offset()] = attempt
					delay := s.retryInitial
					for i := 1; i < attempt; i++ {
						delay = time.Duration(float64(delay) * s.retryMultiplier)
						if delay > s.retryMax {
							delay = s.retryMax
							break
						}
					}
					q.nextReadyAt = time.Now().Add(delay)
				}
				s.aggMu.Unlock()
				processedOne = true
				break
			}

			// Success
			atomic.AddInt64(&s.processedCount, 1)
			logrus.WithFields(logrus.Fields{
				"consumer":  s.name,
				"offset":    ev.Offset(),
				"aggregate": aggID,
			}).Debug("event processed successfully; acknowledging")
			s.tryAck(ev.Offset())
			// Remove from queue and clear attempt for this offset
			s.aggMu.Lock()
			q = s.aggQueues[aggID]
			if q != nil && len(q.events) > 0 && q.events[0] == ev {
				q.events = q.events[1:]
				delete(q.attemptsByOf, ev.Offset())
				q.nextReadyAt = time.Time{}
				if len(q.events) == 0 {
					// optional: cleanup to keep map small
					delete(s.aggQueues, aggID)
				}
			}
			s.aggMu.Unlock()
			processedOne = true
			break
		}
		if !processedOne {
			s.aggMu.Unlock()
		} else {
			continue
		}

		// If nothing pulled and nothing processed
		if queueClosed {
			s.aggMu.Lock()
			empty := len(s.aggQueues) == 0
			s.aggMu.Unlock()
			if empty {
				break
			}
		}
		if !pulled && !processedOne {
			// avoid busy spin
			if !sleepCtx(s.ctx, 5*time.Millisecond) {
				break
			}
		}
	}
	// Signal acker to flush remaining offsets and exit
	close(s.ackQueue)
}

// ackLoop aggregates and persists acknowledgments, committing the highest contiguous offset.
func (s *AsyncSequentialSubscriber[S]) ackLoop() {
	defer s.wg.Done()

	logrus.WithFields(logrus.Fields{
		"consumer":        s.name,
		"ackBatchSize":    s.ackBatchSize,
		"ackBatchTimeout": s.ackBatchTimeout,
	}).Debug("ack loop started")

	// Ack batching semantics:
	// - If ackBatchTimeout > 0: flush when batch is full OR timer fires
	// - If ackBatchTimeout == 0: flush on every ack
	// Maintain a set of seen offsets across drains so out-of-order acks still advance contiguously
	seen := make(map[int]struct{})
	pendingAdded := 0
	var timer *time.Timer
	var timerCh <-chan time.Time
	if s.ackBatchTimeout > 0 {
		timer = time.NewTimer(s.ackBatchTimeout)
		timerCh = timer.C
		defer timer.Stop()
	}

	reset := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(s.ackBatchTimeout)
	}

	drain := func() {
		if len(seen) == 0 {
			return
		}
		all := make([]int, 0, len(seen))
		for o := range seen {
			all = append(all, o)
		}
		logrus.WithFields(logrus.Fields{
			"consumer": s.name,
			"count":    len(all),
		}).Debug("ack loop: draining pending acknowledgments")
		if committedUpTo, committed := s.processBatchedAcks(all); committed {
			for o := range seen {
				if o <= committedUpTo {
					delete(seen, o)
				}
			}
		}
		pendingAdded = 0
	}

	for {
		select {
		case offset, ok := <-s.ackQueue:
			if !ok {
				drain()
				return
			}
			if _, exists := seen[offset]; !exists {
				seen[offset] = struct{}{}
				pendingAdded++
			}
			logrus.WithFields(logrus.Fields{
				"consumer": s.name,
				"offset":   offset,
				"pending":  len(seen),
			}).Debug("ack loop: received acknowledgment offset")
			if pendingAdded >= s.ackBatchSize || timer == nil {
				drain()
				if timer != nil {
					reset()
				}
			}
		case <-timerCh:
			drain()
			reset()
		}
	}
}

// ackEvent acknowledges a single event offset in the database with retry on serialization conflicts.
func (s *AsyncSequentialSubscriber[S]) ackEvent(offset int) {
	const maxRetries = 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		tx, err := s.transactional.BeginTransaction(s.ctx, transactional.BeginTransactionOptions{
			AccessMode:     transactional.ReadWrite,
			IsolationLevel: transactional.Serializable,
			DeferrableMode: transactional.NotDeferrable,
		})
		if err != nil {
			logrus.WithError(err).Error("ack transaction begin failed")
			return
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		consumer, err := s.consumerStore.Load(s.ctx, tx, s.name)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"consumer": s.name,
			}).WithError(err).Error("load consumer state failed")
			return
		}

		if offset == consumer.OffsetAcked()+1 {
			// Never move consumed backwards; keep the max of current consumed and new acked offset
			newConsumed := consumer.OffsetConsumed()
			if offset > newConsumed {
				newConsumed = offset
			}
			if err := s.consumerStore.Save(s.ctx, tx, s.name, offset, newConsumed); err != nil {
				if isSerializableErr(err) {
					_ = tx.Rollback()
					time.Sleep(30 * time.Millisecond)
					continue
				}
				logrus.WithFields(logrus.Fields{
					"consumer": s.name,
					"offset":   offset,
				}).WithError(err).Error("save acked offset failed")
				return
			}
			// If we caught up to the reserved end, clear reservation flag
			s.reservationMu.Lock()
			if newConsumed >= s.reservedEnd && offset >= s.reservedEnd {
				s.reservationActive = false
			}
			s.reservationMu.Unlock()
		}

		if err := tx.Commit(); err != nil {
			if isSerializableErr(err) {
				_ = tx.Rollback()
				time.Sleep(30 * time.Millisecond)
				continue
			}
			logrus.WithError(err).Error("ack transaction commit failed")
			return
		}
		committed = true
		return
	}
}

// processBatchedAcks processes a batch of acknowledgment offsets and computes the highest contiguous
// offset that can be committed. It returns (ackedUpTo, committed), where committed indicates whether
// a transaction was successfully committed (even if no progress was made).
func (s *AsyncSequentialSubscriber[S]) processBatchedAcks(offsets []int) (int, bool) {
	if len(offsets) == 0 {
		return 0, false
	}

	const maxRetries = 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Start transaction for batch update
		tx, err := s.transactional.BeginTransaction(s.ctx, transactional.BeginTransactionOptions{
			AccessMode:     transactional.ReadWrite,
			IsolationLevel: transactional.Serializable,
			DeferrableMode: transactional.NotDeferrable,
		})
		if err != nil {
			logrus.WithError(err).Error("batch ack transaction begin failed")
			// Fall back to individual acks with sort + dedupe to maximize forward progress
			uniq := make(map[int]struct{}, len(offsets))
			for _, o := range offsets {
				uniq[o] = struct{}{}
			}
			sorted := make([]int, 0, len(uniq))
			for o := range uniq {
				sorted = append(sorted, o)
			}
			sort.Ints(sorted)
			for _, o := range sorted {
				s.ackEvent(o)
			}
			return 0, false
		}

		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		// Load current consumer state
		consumer, err := s.consumerStore.Load(s.ctx, tx, s.name)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"consumer": s.name,
			}).WithError(err).Error("load consumer state for batch ack failed")
			return 0, false
		}

		// Find highest contiguous offset we can acknowledge
		expectedNext := consumer.OffsetAcked() + 1
		highestContiguous := consumer.OffsetAcked()

		// Sort offsets to process them in order
		sortedOffsets := make([]int, len(offsets))
		copy(sortedOffsets, offsets)
		sort.Ints(sortedOffsets)

		// Bootstrap for any starting offset (retention / first-run / re-seed):
		// If the first seen offset is ahead of expectedNext, advance starting point
		if len(sortedOffsets) > 0 {
			minSeen := sortedOffsets[0]
			if minSeen > expectedNext {
				logrus.WithFields(logrus.Fields{
					"consumer":        s.name,
					"acked_prev":      consumer.OffsetAcked(),
					"consumed_prev":   consumer.OffsetConsumed(),
					"bootstrap_start": minSeen,
					"expected_prev":   expectedNext,
				}).Debug("batch ack bootstrap: advancing start to first seen offset")
				expectedNext = minSeen
				highestContiguous = minSeen - 1
			}
		}

		// If stream is 0-based and consumer starts at 0 with first seen 0, allow starting at 0
		if consumer.OffsetAcked() == 0 && len(sortedOffsets) > 0 && sortedOffsets[0] == 0 {
			expectedNext = 0
			highestContiguous = -1
		}

		// Find the highest contiguous offset
		for _, offset := range sortedOffsets {
			if offset == expectedNext {
				highestContiguous = offset
				expectedNext++
			} else if offset > expectedNext {
				// Gap found, stop here
				break
			}
			// If offset < expectedNext, it's a duplicate or out-of-order, skip it
		}

		// Only update if we found new contiguous offsets
		if highestContiguous > consumer.OffsetAcked() {
			// Never move consumed backwards; keep max(consumed, highestContiguous)
			newConsumed := consumer.OffsetConsumed()
			if highestContiguous > newConsumed {
				newConsumed = highestContiguous
			}
			if err := s.consumerStore.Save(s.ctx, tx, s.name, highestContiguous, newConsumed); err != nil {
				if isSerializableErr(err) {
					_ = tx.Rollback()
					time.Sleep(30 * time.Millisecond)
					continue
				}
				logrus.WithFields(logrus.Fields{
					"consumer":  s.name,
					"ackedUpTo": highestContiguous,
				}).WithError(err).Error("save batched acked offset failed")
				return 0, false
			}

			if err := tx.Commit(); err != nil {
				if isSerializableErr(err) {
					_ = tx.Rollback()
					time.Sleep(30 * time.Millisecond)
					continue
				}
				logrus.WithError(err).Error("batch ack transaction commit failed")
				return 0, false
			}
			committed = true

			// If we caught up to the reserved end, clear reservation flag
			s.reservationMu.Lock()
			if newConsumed >= s.reservedEnd && highestContiguous >= s.reservedEnd {
				s.reservationActive = false
			}
			s.reservationMu.Unlock()
			logrus.WithFields(logrus.Fields{
				"consumer":       s.name,
				"ackedUpTo":      highestContiguous,
				"offsetsInBatch": len(offsets),
			}).Debug("batch acknowledgment committed")
			return highestContiguous, true
		} else {
			// No contiguous progress possible; likely a gap at expectedNext
			if err := tx.Commit(); err != nil {
				if isSerializableErr(err) {
					_ = tx.Rollback()
					time.Sleep(30 * time.Millisecond)
					continue
				}
				logrus.WithError(err).Error("empty batch ack transaction commit failed")
			}
			return highestContiguous, true
		}
	}
	return 0, false
}

// RegisterHandler registers a handler for the given event type, replacing any existing handler.
func (s *AsyncSequentialSubscriber[S]) RegisterHandler(eventType string, handler Handler[S]) {
	s.handlersLock.Lock()
	defer s.handlersLock.Unlock()
	s.handlers[eventType] = handler
}

// UnregisterHandler removes any handler registered for the given event type.
func (s *AsyncSequentialSubscriber[S]) UnregisterHandler(eventType string) error {
	s.handlersLock.Lock()
	defer s.handlersLock.Unlock()
	delete(s.handlers, eventType)
	return nil
}

// Status returns current runtime metrics for this subscriber instance.
func (s *AsyncSequentialSubscriber[S]) Status() ConsumerStatus {
	return ConsumerStatus{
		ProcessedEventCount: int(atomic.LoadInt64(&s.processedCount)),
	}
}

// SetHandlerTimeout configures a per-event handler timeout. Set to 0 to disable.
func (s *AsyncSequentialSubscriber[S]) SetHandlerTimeout(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.handlerTimeout = d
}

// eventTypesProcessing returns the set of event types that have registered handlers.
func (s *AsyncSequentialSubscriber[S]) eventTypesProcessing() []string {
	s.handlersLock.RLock()
	defer s.handlersLock.RUnlock()
	keys := make([]string, 0, len(s.handlers))
	for k := range s.handlers {
		keys = append(keys, k)
	}
	return keys
}

// getHandler returns the registered handler for the given event type, or nil if none.
func (s *AsyncSequentialSubscriber[S]) getHandler(eventType string) Handler[S] {
	s.handlersLock.RLock()
	defer s.handlersLock.RUnlock()
	return s.handlers[eventType]
}

// Close requests a graceful shutdown of this subscriber and waits for background workers to exit.
// Any in-flight events are processed and acknowledged before returning.
func (s *AsyncSequentialSubscriber[S]) Close() {
	s.cancel()
	// Ensure input queue is closed so processLoop can exit even if Start() isn't running
	s.closeInputQueue()
	s.wg.Wait()
}

// DefaultConsumerStoreProvider can be set by a storage package (e.g., pgeventconsumer)
// to provide a default ConsumerStore when none is supplied in params.
var DefaultConsumerStoreProvider func() ConsumerStore
