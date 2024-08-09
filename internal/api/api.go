package api

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/internetarchive/Zeno/internal/log"
	"github.com/internetarchive/Zeno/internal/stats"
	"github.com/internetarchive/Zeno/internal/worker"
)

type Config struct {
	Job              string
	APIPort          string
	Prometheus       bool
	PrometheusPrefix string
	StartTime        time.Time
	WorkerPool       *worker.Pool

	PromIncreaser chan struct{}

	Logger *log.Logger
}

type api struct {
	job           string
	port          string
	startTime     time.Time
	usePrometheus bool
	workerPool    *worker.Pool

	// API server
	server   *http.Server
	serverWG *sync.WaitGroup

	logger *log.FieldedLogger

	// Prometheus
	prometheusExporter *PrometheusMetrics
	promIncreaser      chan struct{}
	stopPromIncreaser  chan struct{}
	donePromIncreaser  chan struct{}
}

var (
	isInit     = false
	packageAPI *api
)

func Init(config *Config) {
	newAPI := &api{
		job:           config.Job,
		port:          config.APIPort,
		startTime:     config.StartTime,
		usePrometheus: config.Prometheus,
		workerPool:    config.WorkerPool,
		logger:        config.Logger.WithFields(map[string]interface{}{"module": "api"}),
		server: &http.Server{
			Addr: ":" + config.APIPort,
		},
		serverWG: &sync.WaitGroup{},
	}

	if !isInit {
		isInit = true
		if config.Prometheus {
			newAPI.prometheusExporter = &PrometheusMetrics{
				Prefix: config.PrometheusPrefix,
			}
			newAPI.promIncreaser = config.PromIncreaser
		}
		packageAPI = newAPI
		go watchPromIncreaser()
		packageAPI.serverWG.Add(1)
		go startAPI()
	}
}

// Stop stops the API server and handle the closing of PromIncreaser channel
func Stop() {
	if packageAPI.usePrometheus {
		packageAPI.stopPromIncreaser <- struct{}{}
		<-packageAPI.donePromIncreaser
		close(packageAPI.donePromIncreaser)
	}
	packageAPI.server.Close()
	packageAPI.serverWG.Wait()
}

// startAPI starts the API server for the crawl
func startAPI() {
	defer packageAPI.serverWG.Done()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		crawledSeeds := stats.GetCrawledSeeds()
		crawledAssets := stats.GetCrawledAssets()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response := map[string]interface{}{
			"rate":          stats.GetURIPerSecond(),
			"crawled":       crawledSeeds + crawledAssets,
			"crawledSeeds":  crawledSeeds,
			"crawledAssets": crawledAssets,
			"queued":        stats.GetQueueTotalElementsCount(),
			"uptime":        time.Since(packageAPI.startTime).String(),
		}

		json.NewEncoder(w).Encode(response)
	})

	if packageAPI.usePrometheus {
		http.HandleFunc("/metrics", setupPrometheus().ServeHTTP)
	}

	http.HandleFunc("/queue", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats.GetJSONQueueStats())
	})

	http.HandleFunc("/workers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		workersState := packageAPI.workerPool.GetWorkerStateFromPool("")
		json.NewEncoder(w).Encode(workersState)
	})

	http.HandleFunc("/worker/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		workerID := strings.TrimPrefix(r.URL.Path, "/worker/")
		workersState := packageAPI.workerPool.GetWorkerStateFromPool(workerID)
		if workersState == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "Worker not found",
			})
			return
		}

		json.NewEncoder(w).Encode(workersState)
	})

	err := packageAPI.server.ListenAndServe()
	if err != nil {
		packageAPI.logger.Error("unable to start API", "error", err)
	}
}
