package worker

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/capture"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/log"
	"github.com/internetarchive/Zeno/internal/stats"
)

type status int

const (
	idle status = iota
	paused
	processing
	completed
)

func (s status) String() string {
	statusStr := map[status]string{
		idle:       "idle",
		processing: "processing",
		completed:  "completed",
	}
	return statusStr[s]
}

type workerState struct {
	currentItem  *item.Item
	previousItem *item.Item
	status       status
	lastAction   string
	lastError    error
	lastSeen     time.Time
}

type Worker struct {
	sync.Mutex
	ID     uuid.UUID
	state  *workerState
	stop   chan struct{}
	done   chan struct{}
	item   chan *item.Item
	pool   *Pool
	logger *log.FieldedLogger
}

func (wp *Pool) newWorker() *Worker {
	UUID := uuid.New()
	worker := &Worker{
		ID: UUID,
		logger: wp.logger.WithFields(map[string]interface{}{
			"worker": UUID,
		}), // This is a bit weird but it provides every worker with a logger that has the worker UUID
		state: &workerState{
			status:       idle,
			previousItem: nil,
			currentItem:  nil,
			lastError:    nil,
		},
		stop: make(chan struct{}),
		done: make(chan struct{}),
		item: make(chan *item.Item),

		pool: wp,
	}

	_, loaded := wp.Workers.LoadOrStore(UUID, worker)
	if loaded {
		panic("Worker UUID already exists, wtf?")
	}

	wp.addConsumer(worker.ID, worker.item)

	return worker
}

// Run is the key component of a crawl, it's a background processed dispatched
// when the crawl starts, it listens on a channel to get new URLs to archive,
// and eventually push newly discovered URLs back in the queue.
func (w *Worker) Run() {
	// Start archiving the URLs!
	for {
		w.Lock()
		if w.pool.paused.Load() {
			w.logger.Info("Worker paused")
			w.state.status = paused
			w.state.lastAction = "waiting for crawl to resume"
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if w.pool.canDequeue.Load() {
			w.state.lastAction = "waiting for queue to be available"
			w.state.status = idle
			time.Sleep(10 * time.Millisecond)
			continue
		}

		w.state.lastAction = "waiting for next action"
		select {
		case <-w.stop:
			w.pool.removeConsumer(w.ID)
			close(w.item)
			close(w.stop)
			w.state.currentItem = nil
			w.state.status = completed
			w.logger.Info("Worker stopped")
			stats.DecreaseTotalWorkers()
			w.done <- struct{}{}
			return
		case item := <-w.item:
			w.state.lastAction = "got item"
			if item == nil {
				w.state.lastAction = "item is nil"
				continue
			}
			// Launches the capture of the given item
			w.state.lastAction = "starting capture"
			w.unsafeCapture(item)
		default:
		}
		w.Unlock()
	}
}

// unsafeCapture is named like so because it should only be called when the worker is locked
func (w *Worker) unsafeCapture(item *item.Item) {
	if item == nil {
		return
	}

	// Signals that the worker is processing an item
	stats.IncreaseActiveWorkers()
	w.state.currentItem = item
	w.state.status = processing

	// Capture the item
	w.state.lastAction = "capturing item"
	w.state.lastError = capture.Capture(item)

	// Signals that the worker has finished processing the item
	w.state.lastAction = "finished capturing"
	w.state.status = idle
	w.state.currentItem = nil
	w.state.previousItem = item
	stats.DecreaseActiveWorkers()
	w.state.lastSeen = time.Now()
}
