package hq

import (
	"math"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.archive.org/wb/gocrawlhq"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/log"
	"github.com/internetarchive/Zeno/internal/queue"
	"github.com/internetarchive/Zeno/internal/stats"
	"github.com/internetarchive/Zeno/internal/utils"
)

type operator struct {
	hqClient          *gocrawlhq.Client
	hqProducerChannel chan *item.Item
	hqFinishedChannel chan *item.Item
	hqProject         string
	hqStrategy        string
	job               string
	workerCount       uint
	batchSize         int
	continuousPull    bool

	// Queue
	queue *queue.PersistentGroupedQueue

	// Routines
	stopConsumer  chan struct{}
	doneConsumer  chan struct{}
	stopProducer  chan struct{}
	doneProducer  chan struct{}
	stopFinisher  chan struct{}
	doneFinisher  chan struct{}
	stopWebsocket chan struct{}
	doneWebsocket chan struct{}

	// Logger
	logger *log.FieldedLogger
}

type Config struct {
	HQClient          *gocrawlhq.Client
	HQProducerChannel chan *item.Item
	HQFinishedChannel chan *item.Item
	HQProject         string
	HQStrategy        string
	Job               string
	HQBatchSize       int
	WorkerCount       uint
	ContinuousPull    bool

	Queue *queue.PersistentGroupedQueue

	Logger *log.Logger
}

var (
	isInit          = false
	packageOperator *operator
	// HQChannelsWg    sync.WaitGroup
	Paused *atomic.Bool
)

// Init initializes the hq package
func Init(config *Config) {
	newOperator := &operator{
		hqClient:          config.HQClient,
		hqProducerChannel: config.HQProducerChannel,
		hqFinishedChannel: config.HQFinishedChannel,
		hqProject:         config.HQProject,
		hqStrategy:        config.HQStrategy,
		job:               config.Job,
		stopConsumer:      make(chan struct{}),
		doneConsumer:      make(chan struct{}),
		stopProducer:      make(chan struct{}),
		doneProducer:      make(chan struct{}),
		stopFinisher:      make(chan struct{}),
		doneFinisher:      make(chan struct{}),
		stopWebsocket:     make(chan struct{}),
		doneWebsocket:     make(chan struct{}),
		logger:            config.Logger.WithFields(map[string]interface{}{"module": "hq"}),
		workerCount:       config.WorkerCount,
		batchSize:         config.HQBatchSize,
		queue:             config.Queue,
		continuousPull:    config.ContinuousPull,
	}

	if !isInit {
		isInit = true
		packageOperator = newOperator
		Paused = new(atomic.Bool)
		Paused.Store(false)

		go consumer()
		go producer()
		go finisher()
		go websocket()
	}
}

// Stop stops the hq package
// Stop will close the HQProducerChannel and HQFinishedChannel so you don't have to
func Stop() {
	if !isInit {
		return
	}

	close(packageOperator.hqProducerChannel)
	close(packageOperator.hqFinishedChannel)

	packageOperator.stopConsumer <- struct{}{}
	packageOperator.stopProducer <- struct{}{}
	packageOperator.stopFinisher <- struct{}{}
	packageOperator.stopWebsocket <- struct{}{}

	<-packageOperator.doneConsumer
	<-packageOperator.doneProducer
	<-packageOperator.doneFinisher
	<-packageOperator.doneWebsocket

	close(packageOperator.doneConsumer)
	close(packageOperator.doneProducer)
	close(packageOperator.doneFinisher)
	close(packageOperator.doneWebsocket)
}

// Websocket connects to HQ's websocket and listen for messages.
// It also sends and "identify" message to the HQ to let it know that
// Zeno is connected. This "identify" message is sent every second and
// contains the crawler's stats and details.
func websocket() {
	var (
		// the "identify" message will be sent every second
		// to the crawl HQ
		identifyTicker = time.NewTicker(time.Second)
	)

	defer func() {
		identifyTicker.Stop()
	}()

	// send an "identify" message to the crawl HQ every second
	for {
		select {
		case <-packageOperator.stopWebsocket:
			close(packageOperator.stopWebsocket)
			packageOperator.doneWebsocket <- struct{}{}
			return
		case <-identifyTicker.C:
			err := packageOperator.hqClient.Identify(&gocrawlhq.IdentifyMessage{
				Project:   packageOperator.hqProject,
				Job:       packageOperator.job,
				IP:        utils.GetOutboundIP().String(),
				Hostname:  utils.GetHostname(),
				GoVersion: utils.GetVersion().GoVersion,
			})
			if err != nil {
				packageOperator.logger.Error("error sending identify payload to crawl HQ, trying to reconnect..", "error", err)

				err = packageOperator.hqClient.InitWebsocketConn()
				if err != nil {
					packageOperator.logger.Error("error initializing websocket connection to crawl HQ", "error", err)
				}
			}
		}
	}
}

// Producer send items to HQ
// Use channel HQProducerChannel to send items to HQ
func producer() {
	var (
		discoveredArray   = []gocrawlhq.URL{}
		mutex             = sync.Mutex{}
		terminateProducer = make(chan struct{})
		doneProducer      = make(chan struct{})
	)

	// the discoveredArray is sent to the crawl HQ every 10 seconds
	// or when it reaches a certain size
	go func() {
		HQLastSent := time.Now()

		for {
			select {
			case <-terminateProducer:
				// no need to lock the mutex here, because the producer channel
				// is already closed, so no other goroutine can write to the slice
				if len(discoveredArray) > 0 {
					for {
						_, err := packageOperator.hqClient.Discovered(discoveredArray, "seed", false, false)
						if err != nil {
							packageOperator.logger.Error("error sending payload to crawl HQ, waiting 1s then retrying..", "error", err)
							time.Sleep(time.Second)
							continue
						}
						break
					}
				}
				close(terminateProducer)
				doneProducer <- struct{}{}
				return
			default:
				mutex.Lock()
				if (len(discoveredArray) >= int(math.Ceil(float64(packageOperator.workerCount)/2)) || time.Since(HQLastSent) >= time.Second*10) && len(discoveredArray) > 0 {
					for {
						_, err := packageOperator.hqClient.Discovered(discoveredArray, "seed", false, false)
						if err != nil {
							packageOperator.logger.Error("error sending payload to crawl HQ, waiting 1s then retrying..", "error", err)
							time.Sleep(time.Second)
							continue
						}
						break
					}

					discoveredArray = []gocrawlhq.URL{}
					HQLastSent = time.Now()
				}
				mutex.Unlock()
			}
		}
	}()

	// listen to the discovered channel and add the URLs to the discoveredArray
	for {
		select {
		case <-packageOperator.stopProducer:
			close(packageOperator.stopProducer)
			terminateProducer <- struct{}{}
			<-doneProducer
			close(doneProducer)
			packageOperator.doneProducer <- struct{}{}
			return
		case discoveredItem := <-packageOperator.hqProducerChannel:
			var via string

			if discoveredItem.ParentURL != nil {
				via = utils.URLToString(discoveredItem.ParentURL)
			}

			discoveredURL := gocrawlhq.URL{
				Value: utils.URLToString(discoveredItem.URL),
				Via:   via,
			}

			for i := uint64(0); i < discoveredItem.Hop; i++ {
				discoveredURL.Path += "L"
			}

			// The reason we are using a string instead of a bool is because
			// gob's encode/decode doesn't properly support booleans
			if discoveredItem.BypassSeencheck {
				for {
					_, err := packageOperator.hqClient.Discovered([]gocrawlhq.URL{discoveredURL}, "seed", true, false)
					if err != nil {
						packageOperator.logger.Error("error sending payload to crawl HQ, waiting 1s then retrying..", "error", err, "bypassSeencheck", discoveredItem.BypassSeencheck)
						time.Sleep(time.Second)
						continue
					}
					break
				}
				continue
			}

			mutex.Lock()
			discoveredArray = append(discoveredArray, discoveredURL)
			mutex.Unlock()
		}
	}
}

// Consumer fetch URLs from HQ
func consumer() {
	for {
		select {
		case <-packageOperator.stopConsumer:
			close(packageOperator.stopConsumer)
			packageOperator.doneConsumer <- struct{}{}
			return
		default:
			// This is on purpose evaluated every time,
			// because the value of workers will maybe change
			// during the crawl in the future (to be implemented)
			var batchSize = packageOperator.workerCount
			if packageOperator.batchSize != 0 {
				batchSize = uint(packageOperator.batchSize)
			}

			// If HQContinuousPull is set to true, we will pull URLs from HQ continuously,
			// otherwise we will only pull URLs when needed (and when the crawl is not paused)
			for (uint64(stats.GetQueueTotalElementsCount()) > uint64(batchSize) && !packageOperator.continuousPull) || Paused.Load() { // TODO GET HANDOVER STATE|| c.Queue.HandoverOpen.Get() {
				time.Sleep(time.Millisecond * 50)
				continue
			}

			// get batch from crawl HQ
			batch, err := packageOperator.hqClient.Feed(int(batchSize), packageOperator.hqStrategy)
			if err != nil {
				if strings.Contains(err.Error(), "feed is empty") {
					time.Sleep(time.Second)
				}

				packageOperator.logger.Error("error getting new URLs from crawl HQ", "error", err, "batchSize", batchSize)
				continue
			}

			// send all URLs received in the batch to the queue
			var items = make([]*item.Item, 0, len(batch.URLs))
			if len(batch.URLs) > 0 {
				for _, URL := range batch.URLs {
					newURL, err := url.Parse(URL.Value)
					if err != nil {
						packageOperator.logger.Error("unable to parse URL received from crawl HQ, discarding", "error", err, "url", URL.Value)
						continue
					}

					newItem, err := item.New(newURL, nil, item.TypeSeed, uint64(strings.Count(URL.Path, "L")), URL.ID, false)
					if err != nil {
						packageOperator.logger.Error("unable to create new item from URL received from crawl HQ, discarding", "error", err, "url", URL.Value)
						continue
					}

					items = append(items, newItem)
				}
			}

			err = packageOperator.queue.BatchEnqueue(items...)
			if err != nil {
				packageOperator.logger.Error("unable to enqueue URL batch received from crawl HQ, discarding", "error", err)
				continue
			}
		}
	}
}

// finisher send finished URLs to HQ to mark them as finished
func finisher() {
	var (
		finishedArray       = []gocrawlhq.URL{}
		locallyCrawledTotal int
	)

	for {
		select {
		case <-packageOperator.stopFinisher:
			close(packageOperator.stopFinisher)
			packageOperator.doneFinisher <- struct{}{}
			return
		case finishedItem := <-packageOperator.hqFinishedChannel:
			if finishedItem.ID == "" {
				packageOperator.logger.Warn("URL has no ID, discarding", "url", finishedItem.URL)
				continue
			}

			locallyCrawledTotal += int(finishedItem.LocallyCrawled)
			finishedArray = append(finishedArray, gocrawlhq.URL{ID: finishedItem.ID, Value: utils.URLToString(finishedItem.URL)})

			if len(finishedArray) == int(math.Ceil(float64(packageOperator.workerCount)/2)) {
				for {
					_, err := packageOperator.hqClient.Finished(finishedArray, locallyCrawledTotal)
					if err != nil {
						packageOperator.logger.Error("error sending payload to crawl HQ, waiting 1s then retrying..", "error", err, "finishedArray", finishedArray)
						time.Sleep(time.Second)
						continue
					}
					break
				}

				finishedArray = []gocrawlhq.URL{}
				locallyCrawledTotal = 0
			}
		}

		// send remaining finished URLs
		if len(finishedArray) > 0 {
			for {
				_, err := packageOperator.hqClient.Finished(finishedArray, locallyCrawledTotal)
				if err != nil {
					packageOperator.logger.Error("error submitting finished urls to crawl HQ. retrying in one second...", "error", err, "finishedArray", finishedArray)
					time.Sleep(time.Second)
					continue
				}
				break
			}
		}
	}
}

// SeencheckURLs checks if the URLs are already seen by the HQ
func SeencheckURLs(URLs []*url.URL) (seencheckedBatch []*url.URL, err error) {
	var (
		discoveredURLs []gocrawlhq.URL
	)

	for _, URL := range URLs {
		discoveredURLs = append(discoveredURLs, gocrawlhq.URL{
			Value: utils.URLToString(URL),
		})
	}

	discoveredResponse, err := packageOperator.hqClient.Discovered(discoveredURLs, "asset", false, true)
	if err != nil {
		packageOperator.logger.Error("error sending seencheck payload to crawl HQ", "error", err, "batchLen", len(URLs), "urls", discoveredURLs)
		return seencheckedBatch, err
	}

	if discoveredResponse.URLs != nil {
		for _, URL := range discoveredResponse.URLs {
			// the returned payload only contain new URLs to be crawled by Zeno
			newURL, err := url.Parse(URL.Value)
			if err != nil {
				packageOperator.logger.Error("error parsing URL from HQ seencheck response", "error", err, "url", URL.Value)
				return seencheckedBatch, err
			}

			seencheckedBatch = append(seencheckedBatch, newURL)
		}
	}

	return seencheckedBatch, nil
}

// SeencheckURL checks if the URL is already seen by the HQ
func SeencheckURL(URL *url.URL) (bool, error) {
	discoveredURL := gocrawlhq.URL{
		Value: utils.URLToString(URL),
	}

	discoveredResponse, err := packageOperator.hqClient.Discovered([]gocrawlhq.URL{discoveredURL}, "asset", false, true)
	if err != nil {
		packageOperator.logger.Error("error sending seencheck payload to crawl HQ", "error", err, "url", URL)
		return false, err
	}

	if discoveredResponse.URLs != nil {
		for _, URL := range discoveredResponse.URLs {
			if URL.Value == discoveredURL.Value {
				return false, nil
			}
		}
	}

	// didn't find the URL in the HQ, so it's new and has been added to HQ's seencheck database
	return true, nil
}
