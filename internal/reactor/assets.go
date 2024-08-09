package reactor

import (
	"sync"

	"github.com/internetarchive/Zeno/internal/item"
)

// AssetsWithConcurrency is a struct that holds a parent item and a list of assets to crawl with a specified concurrency
type AssetsWithConcurrency struct {
	ParentItem  *item.Item
	Assets      []*item.Item
	Concurrency int
}

func CrawlAssetsWithConcurrency(awc *AssetsWithConcurrency) {
	totalAssets := len(awc.Assets)
	for i := 0; i < totalAssets; i += awc.Concurrency {
		end := i + awc.Concurrency
		if end > totalAssets {
			end = totalAssets
		}

		var wg sync.WaitGroup
		for _, asset := range awc.Assets[i:end] {
			wg.Add(1)
			go func(assetItem *item.Item) {
				defer wg.Done()
				packageReactor.captureRx <- asset
			}(asset)
		}
		wg.Wait()
		awc.ParentItem.LocallyCrawled += uint64(end - i)
	}
}
