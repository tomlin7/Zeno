package worker

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/log"
	"github.com/internetarchive/Zeno/internal/stats"
)

// Pool is a collection of workers
type Pool struct {
	*Bus
	Count            uint
	Workers          sync.Map
	StopTimeout      time.Duration
	GarbageCollector chan uuid.UUID
	logger           *log.Logger
}

// NewPool creates a new worker pool
func NewPool(count uint, stopTimeout time.Duration) *Pool {
	logger, created := log.DefaultOrStored()
	if created {
		logger.Error("worker.NewPool() initialized logger with default configuration")
	}
	return &Pool{
		Bus:              newBus(logger),
		Count:            count,
		Workers:          sync.Map{},
		StopTimeout:      stopTimeout,
		GarbageCollector: make(chan uuid.UUID),
		logger:           logger,
	}
}

// Start starts the worker pool
func (wp *Pool) Start() {
	for i := uint(0); i < wp.Count; i++ {
		worker := wp.newWorker()
		wp.logger.Info("Starting worker", "worker", worker.ID)
		go worker.Run()
		stats.IncreaseTotalWorkers()
	}
	go wp.workerWatcher()
}

// workerWatcher is a background process that watches over the workers
// and remove them from the pool when they are done
func (wp *Pool) workerWatcher() {
	for {
		select {
		// Check for finished workers marked for GC and remove them from the pool
		case UUID := <-wp.GarbageCollector:
			_, loaded := wp.Workers.LoadAndDelete(UUID.String())
			if !loaded {
				wp.logger.Error("Worker marked for garbage collection not found in the pool", "worker", UUID)
				continue
			}
			wp.removeConsumer(UUID)
			wp.logger.Info("Worker removed from the pool", "worker", UUID)
			stats.DecreaseTotalWorkers()
		}
	}
}

// Size returns the number of workers in the pool
func (wp *Pool) Size() int {
	var length int
	wp.Workers.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}
