package worker

import (
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/log"
)

type Bus struct {
	//Pause
	Pause   chan struct{}
	Unpause chan struct{}
	paused  *atomic.Bool

	//Stop
	Stop chan struct{}
	Done chan struct{}

	//CanDequeue
	StopDequeue   chan struct{}
	ResumeDequeue chan struct{}
	canDequeue    *atomic.Bool

	//I/O
	Recv chan *item.Item
	Send chan *item.Item

	//Internal
	addConsumerCh    chan *consumer
	removeConsumerCh chan string
	consumers        *atomic.Value //map[uuid.UUID]*consumer
	isConsuming      *atomic.Bool
	logger           *log.FieldedLogger
}

// newBus creates a new Bus instance
func newBus(parentLogger *log.Logger) *Bus {
	fieldedLogger := parentLogger.WithFields(map[string]interface{}{"module": "worker.Bus"})
	bus := &Bus{
		Pause:            make(chan struct{}),
		Unpause:          make(chan struct{}),
		paused:           new(atomic.Bool),
		Stop:             make(chan struct{}),
		Done:             make(chan struct{}),
		StopDequeue:      make(chan struct{}),
		ResumeDequeue:    make(chan struct{}),
		canDequeue:       new(atomic.Bool),
		Recv:             make(chan *item.Item),
		Send:             make(chan *item.Item),
		addConsumerCh:    make(chan *consumer),
		removeConsumerCh: make(chan string),
		consumers:        new(atomic.Value),
		isConsuming:      new(atomic.Bool),
		logger:           fieldedLogger,
	}
	go bus.run()
	return bus
}

func (b *Bus) run() {
	for {
		select {
		case <-b.Pause:
			b.paused.Store(true)
		case <-b.Unpause:
			b.paused.Store(false)
		case <-b.Stop:
			consumers := b.consumers.Load().(map[string]*consumer)
			for _, consumer := range consumers {
				b.logger.Info("Stopping worker/consummer pair", "worker", consumer.id)
				consumer.stop <- struct{}{}
			}
			for _, consumer := range consumers {
				<-consumer.done
				close(consumer.done)
			}
			b.Done <- struct{}{}
			return
		case <-b.StopDequeue:
			b.canDequeue.Store(false)
		case <-b.ResumeDequeue:
			b.canDequeue.Store(true)
		case recvConsumer := <-b.addConsumerCh:
			consumers, ok := b.consumers.Load().(map[string]*consumer)
			newConsumers := make(map[string]*consumer)
			if ok {
				for k, v := range consumers {
					newConsumers[k] = v
				}
			}
			newConsumers[recvConsumer.id] = recvConsumer
			b.consumers.Store(newConsumers)
		case id := <-b.removeConsumerCh:
			consumers := b.consumers.Load().(map[string]*consumer)
			newConsumers := make(map[string]*consumer)
			for consumerID, consumer := range consumers {
				if consumerID != id {
					newConsumers[consumerID] = consumer
				} else {
					consumer.stop <- struct{}{}
					<-consumer.done
					close(consumer.done)
				}
			}
			b.consumers.Store(newConsumers)
		case item := <-b.Recv:
			consumers, ok := b.consumers.Load().(map[string]*consumer)
			if !ok {
				b.Recv <- item
			}
			for _, consumer := range consumers {
				select {
				case consumer.item <- item:
				default:
					// Channel is full, skip
				}
			}
		}
	}
}

type consumer struct {
	id   string
	item chan *item.Item
	next chan struct{}
	stop chan struct{}
	done chan struct{}
}

func (b *Bus) addConsumer(id uuid.UUID, item chan *item.Item) {
	c := &consumer{
		id:   id.String(),
		item: item,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	b.addConsumerCh <- c
	b.isConsuming.Store(true)
}

func (b *Bus) removeConsumer(id uuid.UUID) {
	b.removeConsumerCh <- id.String()
}
