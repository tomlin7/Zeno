// Package crawl handles all the crawling logic for Zeno
package crawl

import (
	"path"
	"sync"
	"time"

	"git.archive.org/wb/gocrawlhq"
	"github.com/internetarchive/Zeno/internal/api"
	"github.com/internetarchive/Zeno/internal/capture"
	"github.com/internetarchive/Zeno/internal/hq"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/queue"
	"github.com/internetarchive/Zeno/internal/reactor"
	"github.com/internetarchive/Zeno/internal/seencheck"
	"github.com/internetarchive/Zeno/internal/stats"
	"github.com/internetarchive/Zeno/internal/utils"
)

// Start fire up the crawling process
func (c *Crawl) Start() (err error) {
	c.StartTime = time.Now()
	c.Paused = new(utils.TAtomBool)
	c.Finished = new(utils.TAtomBool)
	c.HQChannelsWg = new(sync.WaitGroup)

	// Init the stats package
	// If LiveStats enabled : launch the reoutine responsible for printing live stats on the standard output
	if stats.IsInitialized() {
		stats.Reset()
	}
	ok := stats.Init(&stats.Config{
		HandoverUsed:    c.UseHandover,           //Pass the handover enable bool
		LocalDedupeUsed: !c.DisableLocalDedupe,   //Invert the local dedupe disable bool
		CDXDedupeUsed:   c.CDXDedupeServer != "", //Pass true if the CDXDedupeServer address if it's set
	})
	if !ok {
		c.Log.Fatal("unable to init stats")
	}

	stats.SetJob(c.Job)
	stats.SetCrawlState("starting")

	if c.UseLiveStats {
		go stats.Printer()
	}

	// Setup the --crawl-time-limit clock
	if c.CrawlTimeLimit != 0 {
		go func() {
			time.Sleep(time.Second * time.Duration(c.CrawlTimeLimit))
			c.Log.Info("Crawl time limit reached: attempting to finish the crawl.")
			go c.finish()
			time.Sleep((time.Duration(c.MaxCrawlTimeLimit) * time.Second) - (time.Duration(c.CrawlTimeLimit) * time.Second))
			c.Log.Fatal("Max crawl time limit reached, exiting..")
		}()
	}

	// Start the background process that will handle os signals
	// to exit Zeno, like CTRL+C
	go c.setupCloseHandler()

	// Initialize the queue & seencheck
	c.Log.Info("Initializing queue and seencheck..")
	c.Queue, err = queue.NewPersistentGroupedQueue(path.Join(c.JobPath, "queue"), c.UseHandover, c.UseCommit)
	if err != nil || c.Queue == nil {
		c.Log.Fatal("unable to init queue", "error", err)
	}

	c.Seencheck, err = seencheck.New(c.JobPath)
	if err != nil {
		c.Log.Fatal("unable to init seencheck", "error", err)
	}

	// TODO: re-implement host limitation
	// Process responsible for slowing or pausing the crawl
	// when the WARC writing queue gets too big
	// go c.crawlSpeedLimiter()

	// If crawl HQ parameters are specified, then we start the background
	// processes responsible for pulling and pushing seeds from and to HQ
	if c.UseHQ {
		newHQClient, err := gocrawlhq.Init(c.HQKey, c.HQSecret, c.HQProject, c.HQAddress)
		if err != nil {
			c.Log.Fatal("unable to init crawl HQ client", "error", err)
		}

		c.HQProducerChannel = make(chan *item.Item, c.Workers.Count)
		c.HQFinishedChannel = make(chan *item.Item, c.Workers.Count)

		hq.Init(&hq.Config{
			HQClient:          newHQClient,
			HQProducerChannel: c.HQProducerChannel,
			HQFinishedChannel: c.HQFinishedChannel,
			HQProject:         c.HQProject,
			HQStrategy:        c.HQStrategy,
			Job:               c.Job,
			HQBatchSize:       c.HQBatchSize,
			WorkerCount:       c.Workers.Count,
			ContinuousPull:    c.HQContinuousPull,
			Queue:             c.Queue,
			Logger:            c.Log,
		})
	} else {
		// Push the seed list to the queue
		c.Log.Info("Pushing seeds in the local queue..")
		var seedPointers []*item.Item
		for idx, item := range c.SeedList {
			seedPointers = append(seedPointers, &item)

			// We enqueue seeds by batch of 100k
			// Workers will start processing them as soon as one batch is enqueued
			if idx%100000 == 0 {
				c.Log.Info("Enqueuing seeds", "index", idx)
				if err := c.Queue.BatchEnqueue(seedPointers...); err != nil {
					c.Log.Error("unable to enqueue seeds, discarding", "error", err)
				}
				seedPointers = nil
			}
		}
		if len(seedPointers) > 0 {
			if err := c.Queue.BatchEnqueue(seedPointers...); err != nil {
				c.Log.Error("unable to enqueue seeds, discarding", "error", err)
			}
		}

		c.SeedList = nil
		c.Log.Info("All seeds are now in queue")
	}

	if c.API {
		c.PromIncreaser = make(chan struct{}, 10)
		api.Init(&api.Config{
			Job:              c.Job,
			APIPort:          c.APIPort,
			Prometheus:       c.Prometheus,
			PrometheusPrefix: c.PrometheusPrefix,
			StartTime:        c.StartTime,
			WorkerPool:       c.Workers,
			PromIncreaser:    c.PromIncreaser,
			Logger:           c.Log,
		})
	}

	// Start the workers pool by building all the workers and starting them
	// Also starts all the background processes that will handle the workers
	c.Workers.Start()

	// Start the reactor
	// The reactor is responsible for dequeueing items, processing them (excluding etc) and sending them to who needs them
	reactor.Init(&reactor.Config{
		UseWorkers:   true,
		WorkerRecvCh: c.Workers.Recv,
		UseQueue:     true,
		Queue:        c.Queue,
		UseHQ:        c.UseHQ,
		HQProducer:   c.HQProducerChannel,
	})

	// Initialize Capture and WARC writer
	c.Log.Info("Initializing WARC writer in capture..")
	captureConfig := &capture.Config{
		WARCPrefix:            c.WARCPrefix,
		WARCPoolSize:          c.WARCPoolSize,
		WARCTempDir:           c.WARCTempDir,
		WARCFullOnDisk:        c.WARCFullOnDisk,
		WARCDedupSize:         c.WARCDedupSize,
		DisableLocalDedupe:    c.DisableLocalDedupe,
		CDXDedupeServer:       c.CDXDedupeServer,
		WARCCustomCookie:      c.WARCCustomCookie,
		CertValidation:        c.CertValidation,
		WARCOperator:          c.WARCOperator,
		JobPath:               c.JobPath,
		DisableAssetsCapture:  c.DisableAssetsCapture,
		DomainsCrawl:          c.DomainsCrawl,
		MaxHops:               uint64(c.MaxHops),
		DisabledHTMLTags:      c.DisabledHTMLTags,
		MaxConcurrentAssets:   c.MaxConcurrentAssets,
		MaxRetry:              c.MaxRetry,
		MaxRedirect:           c.MaxRedirect,
		CaptureAlternatePages: c.CaptureAlternatePages,
		HTTPTimeout:           c.HTTPTimeout,
		Proxy:                 c.Proxy,
		RandomLocalIP:         c.RandomLocalIP,
		UserAgent:             c.UserAgent,
		BypassProxy:           c.BypassProxy,
		ParentLogger:          c.Log,
		UseHQ:                 c.UseHQ,
		UsePrometheus:         c.Prometheus,
		UseSeencheck:          c.UseSeencheck,
	}
	if captureConfig.UseHQ {
		captureConfig.HQFinishedChannel = c.HQFinishedChannel
		captureConfig.HQProducerChannel = c.HQProducerChannel
		captureConfig.HQRateLimitingSendBack = c.HQRateLimitingSendBack
	}
	if captureConfig.UsePrometheus {
		captureConfig.PromIncreaser = c.PromIncreaser
	}
	if captureConfig.UseSeencheck {
		captureConfig.Seencheck = c.Seencheck
	}
	capture.Init(captureConfig)
	c.Log.Info("WARC writer initialized")

	// Start the background process that will periodically check if the disk
	// have enough free space, and potentially pause the crawl if it doesn't
	go c.handleCrawlPause()

	// Set the crawl state to running
	stats.SetCrawlState("running")

	// Start the background process that will catch when there
	// is nothing more to crawl
	if !c.UseHQ {
		c.catchFinish()
	} else {
		for {
			time.Sleep(time.Second)
		}
	}

	return
}
