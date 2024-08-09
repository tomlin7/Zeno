package capture

import (
	"time"

	"github.com/CorentinB/warc"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/log"
	"github.com/internetarchive/Zeno/internal/seencheck"
	"mvdan.cc/xurls/v2"
)

type Config struct {
	// WARC settings
	WARCPrefix         string
	WARCPoolSize       int
	WARCTempDir        string
	WARCFullOnDisk     bool
	WARCDedupSize      int
	DisableLocalDedupe bool
	CDXDedupeServer    string
	WARCCustomCookie   string
	CertValidation     bool

	// WARC rotator settings
	WARCOperator string
	JobPath      string

	// Crawls settings
	DisableAssetsCapture  bool
	DomainsCrawl          bool
	MaxHops               uint64
	DisabledHTMLTags      []string
	MaxConcurrentAssets   int
	MaxRetry              uint8
	MaxRedirect           uint8
	CaptureAlternatePages bool

	// Seencheck settings
	UseSeencheck bool
	Seencheck    *seencheck.Seencheck

	// HTTP settings
	HTTPTimeout   int
	Proxy         string
	RandomLocalIP bool
	UserAgent     string
	BypassProxy   []string

	// Logging
	ParentLogger *log.Logger

	// HQ
	UseHQ                  bool
	HQFinishedChannel      chan *item.Item
	HQProducerChannel      chan *item.Item
	HQRateLimitingSendBack bool

	// Prometheus
	UsePrometheus bool
	PromIncreaser chan struct{}
}

type client struct {
	// HTTP client & WARC
	client                   *warc.CustomHTTPClient
	proxiedClient            *warc.CustomHTTPClient
	stopMonitorWARCWaitGroup chan struct{}
	userAgent                string
	disabledHTMLTags         []string
	bypassProxy              []string

	// HQ
	useHQ                  bool
	hqFinishedChannel      chan *item.Item
	hqProducerChannel      chan *item.Item
	hqRateLimitingSendBack bool

	// local processing
	domainsCrawl          bool
	maxHops               uint64
	disableAssetsCapture  bool
	useSeencheck          bool
	seencheck             *seencheck.Seencheck
	maxConcurrentAssets   int
	maxRetry              uint8
	maxRedirect           uint8
	captureAlternatePages bool

	// Prometheus
	usePrometheus bool
	promIncreaser chan struct{}

	// Internal
	logger *log.FieldedLogger
}

var (
	isinit        = false
	packageClient *client
)

func Init(config *Config) {
	var newClient *client

	regexOutlinks = xurls.Relaxed()

	// Init logger
	fieldedLogger := config.ParentLogger.WithFields(map[string]interface{}{"module": "capture"})

	// Init WARC rotator settings
	rotatorSettings := initWARCRotatorSettings(config)

	dedupeOptions := warc.DedupeOptions{
		LocalDedupe:   !config.DisableLocalDedupe,
		SizeThreshold: config.WARCDedupSize,
	}
	if config.CDXDedupeServer != "" {
		dedupeOptions = warc.DedupeOptions{
			LocalDedupe:   !config.DisableLocalDedupe,
			CDXDedupe:     true,
			CDXURL:        config.CDXDedupeServer,
			CDXCookie:     config.WARCCustomCookie,
			SizeThreshold: config.WARCDedupSize,
		}
	}

	// Init the HTTP client responsible for recording HTTP(s) requests / responses
	HTTPClientSettings := warc.HTTPClientSettings{
		RotatorSettings:     rotatorSettings,
		DedupeOptions:       dedupeOptions,
		DecompressBody:      true,
		SkipHTTPStatusCodes: []int{429},
		VerifyCerts:         config.CertValidation,
		TempDir:             config.WARCTempDir,
		FullOnDisk:          config.WARCFullOnDisk,
		RandomLocalIP:       config.RandomLocalIP,
	}

	newHTTPClient, err := warc.NewWARCWritingHTTPClient(HTTPClientSettings)
	if err != nil {
		fieldedLogger.Fatal("Unable to init WARC writing HTTP client", "error", err)
	}

	go func() {
		for err := range newHTTPClient.ErrChan {
			fieldedLogger.Error("WARC HTTP client error", "error", err)
		}
	}()

	newHTTPClient.Timeout = time.Duration(config.HTTPTimeout) * time.Second
	fieldedLogger.Info("HTTP client timeout set", "timeout_ms", newHTTPClient.Timeout.Milliseconds())

	if config.Proxy != "" {
		proxyHTTPClientSettings := HTTPClientSettings
		proxyHTTPClientSettings.Proxy = config.Proxy

		newProxiedHTTPClient, err := warc.NewWARCWritingHTTPClient(proxyHTTPClientSettings)
		if err != nil {
			fieldedLogger.Fatal("unable to init WARC writing (proxy) HTTP client")
		}

		go func() {
			for err := range newProxiedHTTPClient.ErrChan {
				fieldedLogger.Error("WARC Proxied HTTP client error", "error", err)
			}
		}()

		newClient.proxiedClient = newProxiedHTTPClient
	}

	newClient.client = newHTTPClient

	newClient.useHQ = config.UseHQ
	if newClient.useHQ {
		newClient.hqRateLimitingSendBack = config.HQRateLimitingSendBack
		newClient.hqFinishedChannel = config.HQFinishedChannel
		newClient.hqProducerChannel = config.HQProducerChannel
	}

	newClient.logger = fieldedLogger
	newClient.stopMonitorWARCWaitGroup = make(chan struct{})
	newClient.userAgent = config.UserAgent
	newClient.domainsCrawl = config.DomainsCrawl
	newClient.maxHops = config.MaxHops
	newClient.disableAssetsCapture = config.DisableAssetsCapture
	newClient.useSeencheck = config.UseSeencheck
	newClient.seencheck = config.Seencheck
	newClient.disabledHTMLTags = config.DisabledHTMLTags
	newClient.usePrometheus = config.UsePrometheus
	newClient.maxConcurrentAssets = config.MaxConcurrentAssets
	newClient.bypassProxy = config.BypassProxy
	newClient.maxRetry = config.MaxRetry
	newClient.maxRedirect = config.MaxRedirect

	if newClient.usePrometheus {
		newClient.promIncreaser = config.PromIncreaser
	}

	if !isinit {
		packageClient = newClient
		go packageClient.monitorWARCWaitGroup()
		isinit = true
	} else {
		fieldedLogger.Fatal("Capture package already initialized")
	}
}
