package crawl

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/pkg/frontier"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

const (
	// B represent a Byte
	B = 1
	// KB represent a Kilobyte
	KB = 1024 * B
	// MB represent a MegaByte
	MB = 1024 * KB
	// GB represent a GigaByte
	GB = 1024 * MB
)

type status int

const (
	idle status = iota
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
	currentItem  *frontier.Item
	previousItem *frontier.Item
	status       status
	lastError    error
	lastSeen     time.Time
}

type Worker struct {
	sync.Mutex
	ID         uuid.UUID
	state      *workerState
	doneSignal chan bool
	pool       *WorkerPool
	logger     *log.Entry
}

// Run is the key component of a crawl, it's a background processed dispatched
// when the crawl starts, it listens on a channel to get new URLs to archive,
// and eventually push newly discovered URLs back in the frontier.
func (w *Worker) Run() {
	// Start archiving the URLs!
	for {
		select {
		case <-w.doneSignal:
			w.Lock()
			w.state.currentItem = nil
			w.state.status = completed
			w.logger.Info("Worker stopped")
			return
		case item := <-w.pool.Crawl.Frontier.PullChan:
			// Can it happen? I don't think so but let's be safe
			if item == nil {
				continue
			}
			w.Lock()

			// If the crawl is paused, we wait until it's resumed
			for w.pool.Crawl.Paused.Get() || w.pool.Crawl.Frontier.Paused.Get() {
				time.Sleep(time.Second)
			}

			// If the host of the item is in the host exclusion list, we skip it
			if utils.StringInSlice(item.Host, w.pool.Crawl.ExcludedHosts) || !w.pool.Crawl.checkIncludedHosts(item.Host) {
				if w.pool.Crawl.UseHQ {
					// If we are using the HQ, we want to mark the item as done
					w.pool.Crawl.HQFinishedChannel <- item
				}
				w.Unlock()
				continue
			}

			// Launches the capture of the given item
			w.unsafeCapture(item)
			w.Unlock()
		}
	}
}

// unsafeCapture is named like so because it should only be called when the worker is locked
func (w *Worker) unsafeCapture(item *frontier.Item) {
	if item == nil {
		return
	}

	// Signals that the worker is processing an item
	w.pool.Crawl.ActiveWorkers.Incr(1)
	w.state.currentItem = item
	w.state.status = processing

	// Capture the item
	err := w.pool.Crawl.Capture(item)
	if err != nil {
		w.unsafePushLastError(err)
	}

	// Signals that the worker has finished processing the item
	w.state.status = idle
	w.state.currentItem = nil
	w.state.previousItem = item
	w.pool.Crawl.ActiveWorkers.Incr(-1)
	w.state.lastSeen = time.Now()
}

func (w *Worker) Stop() {
	w.doneSignal <- true
	for w.state.status != completed {
		time.Sleep(5 * time.Millisecond)
	}
}

// unsafePushLastError is named like so because it should only be called when the worker is locked
func (w *Worker) unsafePushLastError(err error) {
	w.state.lastError = err
}

func (wp *WorkerPool) NewWorker(crawlParameters *Crawl) *Worker {
	UUID := uuid.New()
	worker := &Worker{
		ID: UUID,
		logger: crawlParameters.Log.WithFields(map[string]interface{}{
			"worker": UUID,
		}), // This is a bit weird but it provides every worker with a logger that has the worker UUID
		state: &workerState{
			status:       idle,
			previousItem: nil,
			currentItem:  nil,
			lastError:    nil,
		},
		doneSignal: make(chan bool, 1),
		pool:       wp,
	}

	_, loaded := wp.Workers.LoadOrStore(UUID, worker)
	if loaded {
		panic("Worker UUID already exists, wtf?")
	}

	return worker
}
