package crawl

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/internetarchive/Zeno/internal/capture"
	"github.com/internetarchive/Zeno/internal/reactor"
	"github.com/internetarchive/Zeno/internal/stats"
)

// catchFinish is running in the background and detect when the crawl need to be terminated
// because it won't crawl anything more. This doesn't apply for Kafka-powered crawls.
func (crawl *Crawl) catchFinish() {

	// ndlr: idk what this does
	for stats.GetCrawledSeeds()+stats.GetCrawledAssets() <= 0 {
		time.Sleep(1 * time.Second)
	}
	//

	for {
		time.Sleep(time.Second * 5)
		if !crawl.UseHQ && stats.GetActiveWorkers() == 0 && stats.GetQueueTotalElementsCount() == 0 && !crawl.Finished.Get() && (stats.GetCrawledSeeds()+stats.GetCrawledAssets() > 0) {
			crawl.Log.Warn("No more items to crawl, finishing..")
			crawl.finish()
		}
	}
}

func (crawl *Crawl) finish() {
	crawl.Finished.Set(true)
	stats.SetCrawlState("finishing")

	crawl.Log.Warn("[QUEUE] Freezing the dequeue")
	crawl.Queue.FreezeDequeue()

	crawl.Log.Warn("[REACTOR] Stopping the reactor")
	reactor.Stop()

	crawl.Log.Warn("[WORKERS] Waiting for workers to finish")
	crawl.Workers.Stop <- struct{}{}
	close(crawl.Workers.Stop)
	<-crawl.Workers.Done
	crawl.Log.Warn("[WORKERS] All workers finished")

	// When all workers are finished, we can safely close the HQ related channels
	if crawl.UseHQ {
		crawl.Log.Warn("[HQ] Waiting for finished channel to be closed")
		close(crawl.HQFinishedChannel)
		crawl.Log.Warn("[HQ] Finished channel closed")

		crawl.Log.Warn("[HQ] Waiting for producer to finish")
		close(crawl.HQProducerChannel)
		crawl.Log.Warn("[HQ] Producer finished")

		crawl.Log.Warn("[HQ] Waiting for all functions to return")
		crawl.HQChannelsWg.Wait()
		crawl.Log.Warn("[HQ] All functions returned")
	}

	crawl.Log.Warn("[WARC] Closing writer(s) and capture..")
	capture.Stop()
	crawl.Log.Warn("[WARC] Writer(s) and capture closed")

	// Closing the queue
	crawl.Queue.Close()
	crawl.Log.Warn("[QUEUE] Queue closed")

	// Closing the seencheck database
	if crawl.UseSeencheck {
		crawl.Seencheck.Close()
		crawl.Log.Warn("[SEENCHECK] Database closed")
	}

	// Closing the stats
	stats.Stop()

	crawl.Log.Warn("Finished!")

	crawl.Log.Warn("Shutting down the logger, bai bai")
	crawl.Log.StopRotation()
	crawl.Log.StopErrorLog()

	os.Exit(0)
}

func (crawl *Crawl) setupCloseHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	crawl.Log.Warn("CTRL+C catched.. cleaning up and exiting.")
	signal.Stop(c)
	crawl.finish()
}
