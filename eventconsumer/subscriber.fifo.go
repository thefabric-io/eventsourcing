package eventconsumer

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/thefabric-io/eventsourcing"
	"github.com/thefabric-io/transactional"
)

type fifoConsumer[S eventsourcing.AggregateState] struct {
	transactional    transactional.Transactional
	name             string
	consumerStore    ConsumerStore
	eventStore       eventsourcing.EventStore[S]
	handlers         map[string]Handler[S]
	handlersLock     sync.RWMutex
	processedCount   int
	batchSize        int
	waitTime         time.Duration
	waitTimeIfEvents time.Duration
	isolationLevel   transactional.TxIsoLevel
}

type NewFIFOSubscriberParams[S eventsourcing.AggregateState] struct {
	Name             string
	ConsumerStore    ConsumerStore
	EventStore       eventsourcing.EventStore[S]
	BatchSize        int
	WaitTime         time.Duration
	WaitTimeIfEvents time.Duration
	isolationLevel   transactional.TxIsoLevel
}

func NewFIFOConsumer[S eventsourcing.AggregateState](_ context.Context, tx transactional.Transactional, p NewFIFOSubscriberParams[S]) Subscriber[S] {
	s := &fifoConsumer[S]{
		transactional:    tx,
		name:             p.Name,
		consumerStore:    p.ConsumerStore,
		eventStore:       p.EventStore,
		handlers:         make(map[string]Handler[S]),
		handlersLock:     sync.RWMutex{},
		processedCount:   0,
		batchSize:        p.BatchSize,
		waitTime:         p.WaitTime,
		waitTimeIfEvents: p.WaitTimeIfEvents,
		isolationLevel:   transactional.RepeatableRead,
	}

	if len(strings.TrimSpace(p.isolationLevel.String())) > 0 {
		s.isolationLevel = p.isolationLevel
	}

	return s
}

func (pb *fifoConsumer[S]) Start(ctx context.Context) error {
	logrus.Info(pb, "fifoConsumer started")

	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					logrus.Errorf("panic recovered in fifoConsumer: %v", r)
					pb.wait("panic recovered, restarting loop")
				}
			}()

			if ctx.Err() != nil {
				return
			}

			tx, err := pb.transactional.BeginTransaction(ctx, transactional.BeginTransactionOptions{
				AccessMode:     transactional.ReadWrite,
				IsolationLevel: transactional.Serializable,
				DeferrableMode: transactional.NotDeferrable,
			})
			if err != nil {
				logrus.Error("error beginning transaction: ", err)
				pb.wait(err.Error())
				return
			}

			eventRetrieved, err := pb.processEvents(ctx, tx)
			if err != nil {
				logrus.Error("error processing events: ", err)
				_ = tx.Rollback()
				pb.wait(err.Error())
				return
			}

			if err := tx.Commit(); err != nil {
				logrus.Error("error committing transaction: ", err)
				_ = tx.Rollback()
				pb.wait(err.Error())
				return
			}

			// If no events are retrieved, wait for the specified duration
			if eventRetrieved == 0 {
				time.Sleep(pb.waitTime)
			} else {
				time.Sleep(pb.waitTimeIfEvents)
			}
		}()

		// Check for context cancellation after recovering from panic
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (pb *fifoConsumer[S]) wait(cause string) {
	logrus.Info(pb, cause)

	time.Sleep(pb.waitTime)
}

func (pb *fifoConsumer[S]) processEvents(ctx context.Context, tx transactional.Transaction) (int, error) {
	consumer, err := pb.consumerStore.Load(ctx, tx, pb.name)
	if err != nil {
		return 0, err
	}

	events, err := pb.eventStore.Events(ctx, tx, eventsourcing.EventsParams{
		AfterOffset: consumer.OffsetAcked(),
		TypesOnly:   pb.eventTypesProcessing(),
		Limit:       pb.batchSize,
	})
	if err != nil {
		return 0, err
	}

	eventRetrieved := len(events)

	var lastAckedEventOffset, lastConsumedEventOffset int

	for _, e := range events {
		pb.processedCount++

		handler, exists := pb.handlers[e.Type()]
		if exists {
			lastConsumedEventOffset = e.Offset()

			if err := handler.HandleEvent(ctx, tx, e); err != nil {
				return eventRetrieved, err
			}

			lastAckedEventOffset = e.Offset()
		}
	}

	if eventRetrieved > 0 {
		if err := pb.consumerStore.Save(ctx, tx, pb.name, lastAckedEventOffset, lastConsumedEventOffset); err != nil {
			logrus.Error(pb, "error updating consumer", err)

			return eventRetrieved, err
		}
	}

	return eventRetrieved, nil
}

func (pb *fifoConsumer[S]) RegisterHandler(eventType string, handler Handler[S]) {
	pb.handlersLock.Lock()
	defer pb.handlersLock.Unlock()

	pb.handlers[eventType] = handler
}

func (pb *fifoConsumer[S]) UnregisterHandler(eventType string) error {
	pb.handlersLock.Lock()
	defer pb.handlersLock.Unlock()

	delete(pb.handlers, eventType)

	return nil
}

func (pb *fifoConsumer[S]) Status() ConsumerStatus {
	return ConsumerStatus{
		ProcessedEventCount: pb.processedCount,
	}
}

func (pb *fifoConsumer[S]) eventTypesProcessing() []string {
	keys := make([]string, 0, len(pb.handlers))
	for k := range pb.handlers {
		keys = append(keys, k)
	}

	return keys
}
