package eventconsumer

import (
	"context"
	"sync"

	"github.com/thefabric-io/eventsourcing"
)

// Manager is responsible for managing subscribers. It provides methods to add and remove subscribers,
// and a method to start the subscribers.
type Manager[S eventsourcing.AggregateState] struct {
	subscribers []Subscriber[S]
}

// NewManager creates a new Manager instance.
func NewManager[S eventsourcing.AggregateState]() *Manager[S] {
	return &Manager[S]{
		subscribers: make([]Subscriber[S], 0),
	}
}

// AddProcessors adds one or more subscribers to the Manager.
func (im *Manager[S]) AddProcessors(processors ...Subscriber[S]) {
	im.subscribers = append(im.subscribers, processors...)
}

func (im *Manager[S]) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for _, c := range im.subscribers {
		wg.Add(1)
		go func(s Subscriber[S]) {
			err := s.Start(ctx)
			if err != nil {
				panic(err)
			}

			defer wg.Done()
		}(c)
	}

	wg.Wait()
}
