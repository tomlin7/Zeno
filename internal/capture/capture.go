package capture

import (
	"time"

	"github.com/CorentinB/warc"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/log"
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

	// HTTP settings
	HTTPTimeout   int
	Proxy         string
	RandomLocalIP bool
	UserAgent     string

	// Logging
	ParentLogger *log.Logger

	// HQ
	UseHQ             bool
	HQFinishedChannel chan *item.Item
	HQProducerChannel chan *item.Item
}

type client struct {
	// HTTP client & WARC
	client                   *warc.CustomHTTPClient
	proxiedClient            *warc.CustomHTTPClient
	stopMonitorWARCWaitGroup chan struct{}
	userAgent                string

	// HQ
	useHQ             bool
	hqFinishedChannel chan *item.Item
	hqProducerChannel chan *item.Item

	// Internal
	logger *log.FieldedLogger
}

var (
	isinit        = false
	packageClient *client
)

func Init(config *Config) {
	var newClient *client

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

		newClient = &client{
			client:                   newHTTPClient,
			proxiedClient:            newProxiedHTTPClient,
			userAgent:                config.UserAgent,
			stopMonitorWARCWaitGroup: make(chan struct{}),
			useHQ:                    config.UseHQ,
			hqFinishedChannel:        config.HQFinishedChannel,
			logger:                   fieldedLogger,
		}
	} else {
		newClient = &client{
			client:                   newHTTPClient,
			userAgent:                config.UserAgent,
			stopMonitorWARCWaitGroup: make(chan struct{}),
			useHQ:                    config.UseHQ,
			hqFinishedChannel:        config.HQFinishedChannel,
			logger:                   fieldedLogger,
		}
	}

	if !isinit {
		packageClient = newClient
		go packageClient.monitorWARCWaitGroup()
		isinit = true
	} else {
		fieldedLogger.Fatal("Capture package already initialized")
	}
}
