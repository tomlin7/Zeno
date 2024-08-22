package crawl

import (
	"math"
	"net/url"
	"strings"
	"sync"
	"time"

	"git.archive.org/wb/gocrawlhq"
	"github.com/internetarchive/Zeno/internal/pkg/queue"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

// This function connects to HQ's websocket and listen for messages.
// It also sends and "identify" message to the HQ to let it know that
// Zeno is connected. This "identify" message is sent every second and
// contains the crawler's stats and details.
func (c *Crawl) HQWebsocket() {
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
		startTime := time.Now()
		err := c.HQClient.Identify(&gocrawlhq.IdentifyMessage{
			Project:   c.HQProject,
			Job:       c.Job,
			IP:        utils.GetOutboundIP().String(),
			Hostname:  utils.GetHostname(),
			GoVersion: utils.GetVersion().GoVersion,
		})
		c.PrometheusMetrics.AverageHQIdentifyRequestDuration.Observe(float64(time.Since(startTime).Milliseconds()))
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{})).Error("error sending identify payload to crawl HQ, trying to reconnect..")

			err = c.HQClient.InitWebsocketConn()
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{})).Error("error initializing websocket connection to crawl HQ")
			}
		}

		<-identifyTicker.C
	}
}

func (c *Crawl) HQProducer() {
	defer c.HQChannelsWg.Done()

	var (
		discoveredArray   = []gocrawlhq.URL{}
		mutex             = sync.Mutex{}
		terminateProducer = make(chan bool)
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
						startTime := time.Now()
						_, err := c.HQClient.Discovered(discoveredArray, "seed", false, false)
						c.PrometheusMetrics.AverageHQDiscoveredRequestDuration.Observe(float64(time.Since(startTime).Milliseconds()))
						if err != nil {
							c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{})).Error("error sending payload to crawl HQ, waiting 1s then retrying..")
							time.Sleep(time.Second)
							continue
						}
						break
					}
				}

				return
			default:
				mutex.Lock()
				if (len(discoveredArray) >= int(math.Ceil(float64(c.Workers.Count)/2)) || time.Since(HQLastSent) >= time.Second*10) && len(discoveredArray) > 0 {
					for {
						startTime := time.Now()
						_, err := c.HQClient.Discovered(discoveredArray, "seed", false, false)
						c.PrometheusMetrics.AverageHQDiscoveredRequestDuration.Observe(float64(time.Since(startTime).Milliseconds()))
						if err != nil {
							c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{})).Error("error sending payload to crawl HQ, waiting 1s then retrying..")
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
	for discoveredItem := range c.HQProducerChannel {
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
				startTime := time.Now()
				_, err := c.HQClient.Discovered([]gocrawlhq.URL{discoveredURL}, "seed", true, false)
				c.PrometheusMetrics.AverageHQDiscoveredRequestDuration.Observe(float64(time.Since(startTime).Milliseconds()))
				if err != nil {
					c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{
						"bypassSeencheck": discoveredItem.BypassSeencheck,
					})).Error("error sending payload to crawl HQ, waiting 1s then retrying..")
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

	// if we are here, it means that the HQProducerChannel has been closed
	// so we need to send the last payload to the crawl HQ
	terminateProducer <- true
}

func (c *Crawl) HQConsumer() {
	for {
		// This is on purpose evaluated every time,
		// because the value of workers will maybe change
		// during the crawl in the future (to be implemented)
		var HQBatchSize = c.Workers.Count

		if c.Finished.Get() {
			c.Log.Error("crawl finished, stopping HQ consumer")
			break
		}

		// If HQContinuousPull is set to true, we will pull URLs from HQ continuously,
		// otherwise we will only pull URLs when needed (and when the crawl is not paused)
		for (uint64(c.Queue.GetStats().TotalElements) > uint64(HQBatchSize) && !c.HQContinuousPull) || c.Paused.Get() || c.Queue.HandoverOpen.Get() {
			time.Sleep(time.Millisecond * 50)
			continue
		}

		// If a specific HQ batch size is set, use it
		if c.HQBatchSize != 0 {
			HQBatchSize = c.HQBatchSize
		}

		// get batch from crawl HQ
		startTime := time.Now()
		batch, err := c.HQClient.Feed(int(HQBatchSize), c.HQStrategy)
		c.PrometheusMetrics.AverageHQFeedRequestDuration.Observe(float64(time.Since(startTime).Milliseconds()))
		if err != nil {
			if strings.Contains(err.Error(), "feed is empty") {
				time.Sleep(time.Second)
			}

			c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{
				"batchSize": HQBatchSize,
				"err":       err,
			})).Error("error getting new URLs from crawl HQ")
			continue
		}

		// send all URLs received in the batch to the queue
		var items = make([]*queue.Item, 0, len(batch.URLs))
		if len(batch.URLs) > 0 {
			for _, URL := range batch.URLs {
				newURL, err := url.Parse(URL.Value)
				if err != nil {
					c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{
						"url":       URL.Value,
						"batchSize": HQBatchSize,
						"err":       err,
					})).Error("unable to parse URL received from crawl HQ, discarding")
					continue
				}

				newItem, err := queue.NewItem(newURL, nil, "seed", uint64(strings.Count(URL.Path, "L")), URL.ID, false)
				if err != nil {
					c.Log.WithFields(c.genLogFields(err, newURL, map[string]interface{}{
						"url":       URL.Value,
						"batchSize": HQBatchSize,
						"err":       err,
					})).Error("unable to create new item from URL received from crawl HQ, discarding")
					continue
				}

				items = append(items, newItem)
			}
		}

		err = c.Queue.BatchEnqueue(items...)
		if err != nil {
			c.Log.Error("unable to enqueue URL batch received from crawl HQ, discarding", "error", err)
			continue
		}
	}
}

func (c *Crawl) HQFinisher() {
	defer c.HQChannelsWg.Done()

	var (
		finishedArray       = []gocrawlhq.URL{}
		locallyCrawledTotal int
	)

	for finishedItem := range c.HQFinishedChannel {
		if finishedItem.ID == "" {
			c.Log.WithFields(c.genLogFields(nil, finishedItem.URL, nil)).Warn("URL has no ID, discarding")
			continue
		}

		locallyCrawledTotal += int(finishedItem.LocallyCrawled)
		finishedArray = append(finishedArray, gocrawlhq.URL{ID: finishedItem.ID, Value: utils.URLToString(finishedItem.URL)})

		if len(finishedArray) == int(math.Ceil(float64(c.Workers.Count)/2)) {
			for {
				startTime := time.Now()
				_, err := c.HQClient.Finished(finishedArray, locallyCrawledTotal)
				c.PrometheusMetrics.AverageHQFinishedRequestDuration.Observe(float64(time.Since(startTime).Milliseconds()))
				if err != nil {
					c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{
						"finishedArray": finishedArray,
					})).Error("error submitting finished urls to crawl HQ. retrying in one second...")
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
			startTime := time.Now()
			_, err := c.HQClient.Finished(finishedArray, locallyCrawledTotal)
			c.PrometheusMetrics.AverageHQFinishedRequestDuration.Observe(float64(time.Since(startTime).Milliseconds()))
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{
					"finishedArray": finishedArray,
				})).Error("error submitting finished urls to crawl HQ. retrying in one second...")
				time.Sleep(time.Second)
				continue
			}
			break
		}
	}
}

func (c *Crawl) HQSeencheckURLs(URLs []*url.URL) (seencheckedBatch []*url.URL, err error) {
	var (
		discoveredURLs []gocrawlhq.URL
	)

	for _, URL := range URLs {
		discoveredURLs = append(discoveredURLs, gocrawlhq.URL{
			Value: utils.URLToString(URL),
		})
	}

	startTime := time.Now()
	discoveredResponse, err := c.HQClient.Discovered(discoveredURLs, "asset", false, true)
	c.PrometheusMetrics.AverageHQSeencheckRequestDuration.Observe(float64(time.Since(startTime).Milliseconds()))
	if err != nil {
		c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{
			"batchLen": len(URLs),
			"urls":     discoveredURLs,
		})).Error("error sending seencheck payload to crawl HQ")
		return seencheckedBatch, err
	}

	if discoveredResponse.URLs != nil {
		for _, URL := range discoveredResponse.URLs {
			// the returned payload only contain new URLs to be crawled by Zeno
			newURL, err := url.Parse(URL.Value)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, URL, map[string]interface{}{
					"batchLen": len(URLs),
				})).Error("error parsing URL from HQ seencheck response")
				return seencheckedBatch, err
			}

			seencheckedBatch = append(seencheckedBatch, newURL)
		}
	}

	return seencheckedBatch, nil
}

func (c *Crawl) HQSeencheckURL(URL *url.URL) (bool, error) {
	discoveredURL := gocrawlhq.URL{
		Value: utils.URLToString(URL),
	}

	startTime := time.Now()
	discoveredResponse, err := c.HQClient.Discovered([]gocrawlhq.URL{discoveredURL}, "asset", false, true)
	c.PrometheusMetrics.AverageHQSeencheckRequestDuration.Observe(float64(time.Since(startTime).Milliseconds()))
	if err != nil {
		c.Log.Error("error sending seencheck payload to crawl HQ", "err", err, "url", utils.URLToString(URL))
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
