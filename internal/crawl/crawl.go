// Package crawl handles all the crawling logic for Zeno
package crawl

import (
	"path"
	"sync"
	"time"

	"git.archive.org/wb/gocrawlhq"
	"github.com/internetarchive/Zeno/internal/capture"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/processer"
	"github.com/internetarchive/Zeno/internal/queue"
	"github.com/internetarchive/Zeno/internal/seencheck"
	"github.com/internetarchive/Zeno/internal/stats"
	"github.com/internetarchive/Zeno/internal/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/telanflow/cookiejar"
	"mvdan.cc/xurls/v2"
)

// PrometheusMetrics define all the metrics exposed by the Prometheus exporter
type PrometheusMetrics struct {
	Prefix        string
	DownloadedURI prometheus.Counter
}

// Start fire up the crawling process
func (c *Crawl) Start() (err error) {
	c.StartTime = time.Now()
	c.Paused = new(utils.TAtomBool)
	c.Finished = new(utils.TAtomBool)
	c.HQChannelsWg = new(sync.WaitGroup)
	regexOutlinks = xurls.Relaxed()

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
	if err != nil {
		c.Log.Fatal("unable to init queue", "error", err)
	}

	c.Seencheck, err = seencheck.New(c.JobPath)
	if err != nil {
		c.Log.Fatal("unable to init seencheck", "error", err)
	}

	// Start the background process that will periodically check if the disk
	// have enough free space, and potentially pause the crawl if it doesn't
	go c.handleCrawlPause()

	// TODO: re-implement host limitation
	// Process responsible for slowing or pausing the crawl
	// when the WARC writing queue gets too big
	// go c.crawlSpeedLimiter()

	// Parse input cookie file if specified
	if c.CookieFile != "" {
		cookieJar, err := cookiejar.NewFileJar(c.CookieFile, nil)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, nil, nil)).Fatal("unable to parse cookie file")
		}

		c.Client.Jar = cookieJar
	}

	// If crawl HQ parameters are specified, then we start the background
	// processes responsible for pulling and pushing seeds from and to HQ
	if c.UseHQ {
		c.HQClient, err = gocrawlhq.Init(c.HQKey, c.HQSecret, c.HQProject, c.HQAddress)
		if err != nil {
			c.Log.Fatal("unable to init crawl HQ client", "error", err)
		}

		c.HQProducerChannel = make(chan *item.Item, c.Workers.Count)
		c.HQFinishedChannel = make(chan *item.Item, c.Workers.Count)

		c.HQChannelsWg.Add(2)
		go c.HQConsumer()
		go c.HQProducer()
		go c.HQFinisher()
		go c.HQWebsocket()
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

	// Initialize WARC writer
	c.Log.Info("Initializing WARC writer in capture..")
	capture.Init(&capture.Config{
		WARCPrefix:         c.WARCPrefix,
		WARCPoolSize:       c.WARCPoolSize,
		WARCTempDir:        c.WARCTempDir,
		WARCFullOnDisk:     c.WARCFullOnDisk,
		WARCDedupSize:      c.WARCDedupSize,
		DisableLocalDedupe: c.DisableLocalDedupe,
		CDXDedupeServer:    c.CDXDedupeServer,
		WARCCustomCookie:   c.WARCCustomCookie,
		CertValidation:     c.CertValidation,
		WARCOperator:       c.WARCOperator,
		JobPath:            c.JobPath,
		HTTPTimeout:        c.HTTPTimeout,
		UserAgent:          c.UserAgent,
		Proxy:              c.Proxy,
		RandomLocalIP:      c.RandomLocalIP,
		ParentLogger:       c.Log,
		UseHQ:              c.UseHQ,
		HQFinishedChannel:  c.HQFinishedChannel,
		HQProducerChannel:  c.HQProducerChannel,
	})
	c.Log.Info("WARC writer initialized")

	if c.API {
		go c.startAPI()
	}

	// Start the workers pool by building all the workers and starting them
	// Also starts all the background processes that will handle the workers
	c.Workers.Start()

	// Start the processer
	// The processer is responsible for dequeueing items, processing them (excluding etc) and sending them to the workers
	processer.Init(c.Workers, c.Queue)

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
