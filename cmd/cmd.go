package cmd

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/CorentinB/Zeno/config"
)

var GlobalFlags = []cli.Flag{
	&cli.StringFlag{
		Name:        "user-agent",
		Value:       "Zeno",
		Usage:       "User agent to use when requesting URLs.",
		Destination: &config.App.Flags.UserAgent,
	},
	&cli.StringFlag{
		Name:        "job",
		Value:       "",
		Usage:       "Job name to use, will determine the path for the persistent queue, seencheck database, and WARC files.",
		Destination: &config.App.Flags.Job,
	},
	&cli.IntFlag{
		Name:        "workers",
		Aliases:     []string{"w"},
		Value:       1,
		Usage:       "Number of concurrent workers to run.",
		Destination: &config.App.Flags.Workers,
	},
	&cli.IntFlag{
		Name:        "max-concurrent-assets",
		Aliases:     []string{"ca"},
		Value:       8,
		Usage:       "Max number of concurrent assets to fetch PER worker. E.g. if you have 100 workers and this setting at 8, Zeno could do up to 800 concurrent requests at any time.",
		Destination: &config.App.Flags.MaxConcurrentAssets,
	},
	&cli.UintFlag{
		Name:        "max-hops",
		Value:       0,
		Usage:       "Maximum number of hops to execute.",
		Destination: &config.App.Flags.MaxHops,
	},
	&cli.StringFlag{
		Name:        "cookies",
		Usage:       "File containing cookies that will be used for requests.",
		Destination: &config.App.Flags.CookieFile,
	},
	&cli.BoolFlag{
		Name:        "keep-cookies",
		Usage:       "Keep a global cookie jar",
		Destination: &config.App.Flags.KeepCookies,
	},
	&cli.BoolFlag{
		Name:        "headless",
		Usage:       "Use headless browsers instead of standard GET requests.",
		Destination: &config.App.Flags.Headless,
	},
	&cli.BoolFlag{
		Name:        "local-seencheck",
		Usage:       "Simple local seencheck to avoid re-crawling of URIs.",
		Destination: &config.App.Flags.Seencheck,
	},
	&cli.BoolFlag{
		Name:        "json",
		Usage:       "Output logs in JSON",
		Destination: &config.App.Flags.JSON,
	},
	&cli.BoolFlag{
		Name:        "debug",
		Destination: &config.App.Flags.Debug,
	},
	&cli.BoolFlag{
		Name:        "live-stats",
		Destination: &config.App.Flags.LiveStats,
	},

	&cli.BoolFlag{
		Name:        "api",
		Destination: &config.App.Flags.API,
	},
	&cli.StringFlag{
		Name:        "api-port",
		Value:       "9443",
		Usage:       "Port to listen on for the API.",
		Destination: &config.App.Flags.APIPort,
	},
	&cli.BoolFlag{
		Name:        "prometheus",
		Destination: &config.App.Flags.Prometheus,
		Usage:       "Export metrics in Prometheus format, using this setting imply --api.",
	},
	&cli.StringFlag{
		Name:        "prometheus-prefix",
		Destination: &config.App.Flags.PrometheusPrefix,
		Usage:       "String used as a prefix for the exported Prometheus metrics.",
		Value:       "zeno:",
	},

	&cli.IntFlag{
		Name:        "max-redirect",
		Value:       20,
		Usage:       "Specifies the maximum number of redirections to follow for a resource.",
		Destination: &config.App.Flags.MaxRedirect,
	},
	&cli.IntFlag{
		Name:        "max-retry",
		Value:       20,
		Usage:       "Number of retry if error happen when executing HTTP request.",
		Destination: &config.App.Flags.MaxRetry,
	},
	&cli.BoolFlag{
		Name:        "domains-crawl",
		Usage:       "If this is turned on, seeds will be treated as domains to crawl, therefore same-domain outlinks will be added to the queue as hop=0.",
		Destination: &config.App.Flags.DomainsCrawl,
	},
	&cli.StringSliceFlag{
		Name:        "disable-html-tag",
		Usage:       "Specify HTML tag to not extract assets from",
		Destination: &config.App.Flags.DisabledHTMLTags,
	},
	&cli.BoolFlag{
		Name:        "capture-alternate-pages",
		Usage:       "If turned on, <link> HTML tags with \"alternate\" values for their \"rel\" attribute will be archived.",
		Destination: &config.App.Flags.CaptureAlternatePages,
	},
	&cli.StringSliceFlag{
		Name:        "exclude-host",
		Usage:       "Exclude a specific host from the crawl, note that it will not exclude the domain if it is encountered as an asset for another web page.",
		Destination: &config.App.Flags.ExcludedHosts,
	},
	&cli.BoolFlag{
		Name:        "disable-pdf-extraction",
		Usage:       "If specified, PDF won't be parsed for URLs.",
		Destination: &config.App.Flags.DisablePDFExtraction,
	},

	// Proxy flags
	&cli.StringFlag{
		Name:        "proxy",
		Value:       "",
		Usage:       "Proxy to use when requesting pages.",
		Destination: &config.App.Flags.Proxy,
	},
	&cli.StringSliceFlag{
		Name:        "bypass-proxy",
		Usage:       "Domains that should not be proxied.",
		Destination: &config.App.Flags.BypassProxy,
	},

	// WARC flags
	&cli.StringFlag{
		Name:        "warc-prefix",
		Value:       "ZENO",
		Usage:       "Prefix to use when naming the WARC files.",
		Destination: &config.App.Flags.WARCPrefix,
	},
	&cli.StringFlag{
		Name:        "warc-operator",
		Value:       "",
		Usage:       "Contact informations of the crawl operator to write in the Warc-Info record in each WARC file.",
		Destination: &config.App.Flags.WARCOperator,
	},
	&cli.StringFlag{
		Name:        "warc-cdx-dedupe-server",
		Value:       "",
		Usage:       "Identify the server to use CDX deduplication. This turns CDX deduplication on.",
		Destination: &config.App.Flags.CDXDedupeServer,
	},
	&cli.BoolFlag{
		Name:        "warc-on-disk",
		Usage:       "Do not use RAM to store payloads when recording traffic to WARCs, everything will happen on disk (usually used to reduce memory usage).",
		Destination: &config.App.Flags.WARCFullOnDisk,
	},
	&cli.IntFlag{
		Name:        "warc-pool-size",
		Value:       1,
		Usage:       "Number of concurrent WARC files to write.",
		Destination: &config.App.Flags.WARCPoolSize,
	},
	&cli.StringFlag{
		Name:        "warc-temp-dir",
		Value:       "",
		Usage:       "Custom directory to use for WARC temporary files.",
		Destination: &config.App.Flags.WARCTempDir,
	},
	&cli.BoolFlag{
		Name:        "disable-local-dedupe",
		Usage:       "Disable local URL agonistic deduplication.",
		Destination: &config.App.Flags.DisableLocalDedupe,
	},
	&cli.BoolFlag{
		Name:        "cert-validation",
		Usage:       "Enables certificate validation on HTTPS requests.",
		Destination: &config.App.Flags.CertValidation,
	},
	&cli.BoolFlag{
		Name:        "disable-assets-capture",
		Usage:       "Disable assets capture.",
		Destination: &config.App.Flags.DisableAssetsCapture,
	},

	// Crawl HQ flags
	&cli.BoolFlag{
		Name:        "hq",
		Usage:       "Use Crawl HQ to pull URLs to process.",
		Destination: &config.App.Flags.UseHQ,
	},
	&cli.StringFlag{
		Name:        "hq-address",
		Usage:       "Crawl HQ address.",
		Destination: &config.App.Flags.HQAddress,
	},
	&cli.StringFlag{
		Name:        "hq-key",
		Usage:       "Crawl HQ key.",
		Destination: &config.App.Flags.HQKey,
	},
	&cli.StringFlag{
		Name:        "hq-secret",
		Usage:       "Crawl HQ secret.",
		Destination: &config.App.Flags.HQSecret,
	},
	&cli.StringFlag{
		Name:        "hq-project",
		Usage:       "Crawl HQ project.",
		Destination: &config.App.Flags.HQProject,
	},
	&cli.StringFlag{
		Name:        "hq-strategy",
		Usage:       "Crawl HQ feeding strategy.",
		Value:       "lifo",
		Destination: &config.App.Flags.HQStrategy,
	},
}

var Commands []*cli.Command

func RegisterCommand(command cli.Command) {
	Commands = append(Commands, &command)
}

func CommandNotFound(c *cli.Context, command string) {
	logrus.Errorf("%s: '%s' is not a %s command. See '%s --help'.", c.App.Name, command, c.App.Name, c.App.Name)
	os.Exit(2)
}
