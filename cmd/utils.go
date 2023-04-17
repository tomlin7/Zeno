package cmd

import (
	"path"
	"time"

	"github.com/CorentinB/Zeno/config"
	"github.com/CorentinB/Zeno/internal/pkg/crawl"
	"github.com/CorentinB/Zeno/internal/pkg/frontier"
	"github.com/google/uuid"
	"github.com/paulbellamy/ratecounter"
	"github.com/remeh/sizedwaitgroup"
	"github.com/sirupsen/logrus"
)

// InitCrawlWithCMD takes a config.Flags struct and return a
// *crawl.Crawl initialized with it
func InitCrawlWithCMD(flags config.Flags) *crawl.Crawl {
	var c = new(crawl.Crawl)

	// Statistics counters
	c.CrawledSeeds = new(ratecounter.Counter)
	c.CrawledAssets = new(ratecounter.Counter)
	c.ActiveWorkers = new(ratecounter.Counter)
	c.URIsPerSecond = ratecounter.NewRateCounter(1 * time.Second)

	c.LiveStats = flags.LiveStats

	// Frontier
	c.Frontier = new(frontier.Frontier)

	// If the job name isn't specified, we generate a random name
	if len(flags.Job) == 0 {
		UUID, err := uuid.NewUUID()
		if err != nil {
			logrus.Fatal(err)
		}
		c.Job = UUID.String()
	} else {
		c.Job = flags.Job
	}
	c.JobPath = path.Join("jobs", flags.Job)

	c.Workers = flags.Workers
	c.WorkerPool = sizedwaitgroup.New(c.Workers)
	c.MaxConcurrentAssets = flags.MaxConcurrentAssets

	c.Seencheck = flags.Seencheck
	c.HTTPTimeout = flags.HTTPTimeout
	c.MaxRetry = flags.MaxRetry
	c.MaxRedirect = flags.MaxRedirect
	c.MaxHops = uint8(flags.MaxHops)
	c.DomainsCrawl = flags.DomainsCrawl
	c.DisableAssetsCapture = flags.DisableAssetsCapture
	c.DisabledHTMLTags = flags.DisabledHTMLTags.Value()
	c.ExcludedHosts = flags.ExcludedHosts.Value()
	c.CaptureAlternatePages = flags.CaptureAlternatePages

	// WARC settings
	c.WARCPrefix = flags.WARCPrefix
	c.WARCOperator = flags.WARCOperator

	if flags.WARCTempDir != "" {
		c.WARCTempDir = flags.WARCTempDir
	} else {
		c.WARCTempDir = path.Join(c.JobPath, "temp")
	}

	c.CDXDedupeServer = flags.CDXDedupeServer
	c.DisableLocalDedupe = flags.DisableLocalDedupe
	c.CertValidation = flags.CertValidation
	c.WARCFullOnDisk = flags.WARCFullOnDisk
	c.WARCPoolSize = flags.WARCPoolSize

	c.API = flags.API
	c.APIPort = flags.APIPort

	// If Prometheus is specified, then we make sure
	// c.API is true
	c.Prometheus = flags.Prometheus
	if c.Prometheus {
		c.API = true
		c.PrometheusMetrics = new(crawl.PrometheusMetrics)
		c.PrometheusMetrics.Prefix = flags.PrometheusPrefix
	}

	c.UserAgent = flags.UserAgent

	c.CookieFile = flags.CookieFile
	c.KeepCookies = flags.KeepCookies

	// Proxy settings
	c.Proxy = flags.Proxy
	c.BypassProxy = flags.BypassProxy.Value()

	// Crawl HQ settings
	c.UseHQ = flags.UseHQ
	c.HQProject = flags.HQProject
	c.HQAddress = flags.HQAddress
	c.HQKey = flags.HQKey
	c.HQSecret = flags.HQSecret
	c.HQStrategy = flags.HQStrategy
	c.HQBatchSize = int(flags.HQBatchSize)
	c.HQContinuousPull = flags.HQContinuousPull

	// Headless settings
	c.Headless = flags.Headless
	c.Headfull = flags.Headfull
	c.HeadlessWaitAfterLoad = flags.HeadlessWaitAfterLoad

	// If Headfull is true, then we make sure Headless is true
	// as well. The reason is that "headless" is (in the context of Zeno)
	// considered as a method of crawling and headfull is considered
	// as a method of rendering for the browser.
	if c.Headfull {
		c.Headless = true
	}

	return c
}
