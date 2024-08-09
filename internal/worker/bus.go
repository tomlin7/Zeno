package worker

import (
	"sync"
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
	consumers        *sync.Map
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
		consumers:        new(sync.Map),
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
			b.consumers.Range(func(key, value interface{}) bool {
				consumer := value.(*consumer)
				b.logger.Info("Stopping worker/consummer pair", "worker", consumer.id)
				consumer.stop <- struct{}{}
				return true
			})
			b.consumers.Range(func(key, value interface{}) bool {
				consumer := value.(*consumer)
				<-consumer.done
				close(consumer.done)
				return true
			})
			b.Done <- struct{}{}
			return
		case <-b.StopDequeue:
			b.canDequeue.Store(false)
		case <-b.ResumeDequeue:
			b.canDequeue.Store(true)
		case recvConsumer := <-b.addConsumerCh:
			b.consumers.Store(recvConsumer.id, recvConsumer)
		case id := <-b.removeConsumerCh:
			loaded, ok := b.consumers.LoadAndDelete(id)
			if ok {
				consumer := loaded.(*consumer)
				consumer.stop <- struct{}{}
				<-consumer.done
				close(consumer.done)
			}
		case item := <-b.Recv:
			b.consumers.Range(func(key, value interface{}) bool {
				consumer := value.(*consumer)
				select {
				case consumer.item <- item:
					return false
				default:
					// Channel is full, skip
				}
				return true
			})
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

func (b *Bus) addConsumer(worker *Worker) {
	c := &consumer{
		id:   worker.ID.String(),
		item: worker.item,
		stop: worker.stop,
		done: worker.done,
	}
	b.addConsumerCh <- c
	b.isConsuming.Store(true)
}

func (b *Bus) removeConsumer(id uuid.UUID) {
	b.removeConsumerCh <- id.String()
}
