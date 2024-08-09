package api

import (
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusMetrics define all the metrics exposed by the Prometheus exporter
type PrometheusMetrics struct {
	Prefix        string
	DownloadedURI prometheus.Counter
}

func setupPrometheus() http.Handler {
	labels := make(map[string]string)

	labels["crawljob"] = packageAPI.job
	hostname, err := os.Hostname()
	if err != nil {
		packageAPI.logger.Error("failed to get hostname", err)
		hostname = "unknown"
	}
	labels["host"] = hostname + ":" + packageAPI.port

	packageAPI.prometheusExporter.DownloadedURI = promauto.NewCounter(prometheus.CounterOpts{
		Name:        packageAPI.prometheusExporter.Prefix + "downloaded_uri_count_total",
		ConstLabels: labels,
		Help:        "The total number of crawled URI",
	})

	packageAPI.logger.Info("starting Prometheus export")

	return promhttp.Handler()
}

func watchPromIncreaser() {
	for {
		select {
		case <-packageAPI.promIncreaser:
			packageAPI.prometheusExporter.DownloadedURI.Inc()
		case <-packageAPI.stopPromIncreaser:
			close(packageAPI.promIncreaser)
			packageAPI.donePromIncreaser <- struct{}{}
			return
		}
	}
}
