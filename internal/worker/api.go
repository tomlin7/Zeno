package worker

import "time"

// APIWorkersState represents the state of all API workers.
type APIWorkersState struct {
	Workers []*APIWorkerState `json:"workers"`
}

// APIWorkerState represents the state of an API worker.
type APIWorkerState struct {
	WorkerID   string `json:"worker_id"`
	Status     string `json:"status"`
	LastError  string `json:"last_error"`
	LastSeen   string `json:"last_seen"`
	LastAction string `json:"last_action"`
	Locked     bool   `json:"locked"`
}

// GetWorkerStateFromPool returns the state of a worker given its index in the worker pool
// if the provided index is -1 then the state of all workers is returned
func (wp *Pool) GetWorkerStateFromPool(UUID string) interface{} {
	if UUID == "" {
		var workersStatus = new(APIWorkersState)
		wp.Workers.Range(func(_, value interface{}) bool {
			workersStatus.Workers = append(workersStatus.Workers, _getWorkerState(value.(*Worker)))
			return true
		})
		return workersStatus
	}
	worker, loaded := wp.Workers.Load(UUID)
	if !loaded {
		return nil
	}
	return _getWorkerState(worker.(*Worker))
}

func _getWorkerState(worker *Worker) *APIWorkerState {
	lastErr := ""
	isLocked := true

	if worker.TryLock() {
		isLocked = false
		worker.Unlock()
	}

	if worker.state.lastError != nil {
		lastErr = worker.state.lastError.Error()
	}

	return &APIWorkerState{
		WorkerID:   worker.ID.String(),
		Status:     worker.state.status.String(),
		LastSeen:   worker.state.lastSeen.Format(time.RFC3339),
		LastError:  lastErr,
		LastAction: worker.state.lastAction,
		Locked:     isLocked,
	}
}
