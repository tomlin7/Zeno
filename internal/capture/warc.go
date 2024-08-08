package capture

import (
	"fmt"
	"path"
	"time"

	"github.com/CorentinB/warc"
	"github.com/internetarchive/Zeno/internal/stats"
	"github.com/internetarchive/Zeno/internal/utils"
)

func initWARCRotatorSettings(config *Config) *warc.RotatorSettings {
	var rotatorSettings = warc.NewRotatorSettings()

	rotatorSettings.OutputDirectory = path.Join(config.JobPath, "warcs")
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = config.WARCPrefix
	rotatorSettings.WarcinfoContent.Set("software", fmt.Sprintf("Zeno %s", utils.GetVersion().Version))
	rotatorSettings.WARCWriterPoolSize = config.WARCPoolSize

	if len(config.WARCOperator) > 0 {
		rotatorSettings.WarcinfoContent.Set("operator", config.WARCOperator)
	}

	return rotatorSettings
}

func (c *client) monitorWARCWaitGroup() {
	// Monitor every 250ms
	ticker := time.NewTicker(250 * time.Millisecond)
	for {
		select {
		case <-c.stopMonitorWARCWaitGroup:
			return
		case <-ticker.C:
			if c.proxiedClient != nil {
				stats.SetWARCWritingQueue(int32(c.client.WaitGroup.Size() + c.proxiedClient.WaitGroup.Size()))
			} else {
				stats.SetWARCWritingQueue(int32(c.client.WaitGroup.Size()))
			}
		}
	}
}
