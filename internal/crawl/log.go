package crawl

import (
	"net/url"
	"sync"

	"github.com/CorentinB/warc"
	"github.com/internetarchive/Zeno/internal/stats"
	"github.com/internetarchive/Zeno/internal/utils"
	"github.com/sirupsen/logrus"
)

var constants sync.Map

func (c *Crawl) genLogFields(err interface{}, URL interface{}, additionalFields map[string]interface{}) (fields logrus.Fields) {
	fields = logrus.Fields{}

	fields["queued"] = stats.GetQueueTotalElementsCount()
	fields["crawled"] = stats.GetCrawledSeeds() + stats.GetCrawledAssets()
	fields["rate"] = stats.GetURIPerSecond()
	fields["activeWorkers"] = stats.GetActiveWorkers()

	ip, found := constants.Load("ip")
	if found {
		fields["ip"] = ip
	} else {
		ip := utils.GetOutboundIP().String()

		// Store local IP address for later log fields to ensure we aren't making an excessive amount of open sockets.
		constants.Store("ip", ip)
		fields["ip"] = ip
	}

	goversion, found := constants.Load("goversion")
	if found {
		fields["goversion"] = goversion
	} else {
		goversion := utils.GetVersion().GoVersion

		// Store version to avoid call to debug.ReadBuildInfo, which I imagine takes more time than a syncmap.
		constants.Store("goversion", goversion)
		fields["goversion"] = goversion
	}

	if c.HQProject != "" {
		fields["hqProject"] = c.HQProject
		fields["hqAddress"] = c.HQAddress
	}

	if c.Job != "" {
		fields["job"] = c.Job
	}

	switch errValue := err.(type) {
	case error:
		fields["err"] = errValue.Error()
	case *warc.Error:
		fields["err"] = errValue.Err.Error()
		fields["errFunc"] = errValue.Func
	default:
	}

	switch URLValue := URL.(type) {
	case string:
		fields["url"] = URLValue
	case *url.URL:
		fields["url"] = utils.URLToString(URLValue)
	case url.URL:
		fields["url"] = utils.URLToString(&URLValue)
	default:
	}

	for key, value := range additionalFields {
		fields[key] = value
	}

	return fields
}
