package crawl

import (
	"fmt"
	"path"
	"time"

	"github.com/CorentinB/warc"
	"github.com/internetarchive/Zeno/internal/stats"
	"github.com/internetarchive/Zeno/internal/utils"
)

func (c *Crawl) initWARCRotatorSettings() *warc.RotatorSettings {
	var rotatorSettings = warc.NewRotatorSettings()

	rotatorSettings.OutputDirectory = path.Join(c.JobPath, "warcs")
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = c.WARCPrefix
	rotatorSettings.WarcinfoContent.Set("software", fmt.Sprintf("Zeno %s", utils.GetVersion().Version))
	rotatorSettings.WARCWriterPoolSize = c.WARCPoolSize

	if len(c.WARCOperator) > 0 {
		rotatorSettings.WarcinfoContent.Set("operator", c.WARCOperator)
	}

	return rotatorSettings
}

func (c *Crawl) monitorWARCWaitGroup() {
	// Monitor every 250ms
	ticker := time.NewTicker(250 * time.Millisecond)
	for {
		select {
		case <-c.stopMonitorWARCWaitGroup:
			return
		case <-ticker.C:
			stats.SetWARCWritingQueue(int32(c.Client.WaitGroup.Size()))
		}
	}
}
