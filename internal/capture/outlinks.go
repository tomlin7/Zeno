package capture

import (
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/reactor"
	"github.com/internetarchive/Zeno/internal/utils"
)

func extractOutlinks(base *url.URL, doc *goquery.Document) (outlinks []*url.URL, err error) {
	var rawOutlinks []string

	// Extract outlinks
	doc.Find("a").Each(func(index int, item *goquery.Selection) {
		link, exists := item.Attr("href")
		if exists {
			rawOutlinks = append(rawOutlinks, link)
		}
	})

	// Extract iframes as 'outlinks' as they usually can be treated as entirely seperate pages with entirely seperate assets.
	doc.Find("iframe").Each(func(index int, item *goquery.Selection) {
		link, exists := item.Attr("src")
		if exists {
			rawOutlinks = append(rawOutlinks, link)
		}
	})

	doc.Find("ref").Each(func(index int, item *goquery.Selection) {
		link, exists := item.Attr("target")
		if exists {
			rawOutlinks = append(rawOutlinks, link)
		}

		fmt.Println(item.Text())
	})

	// Turn strings into url.URL
	outlinks = utils.StringSliceToURLSlice(rawOutlinks)

	// Extract all text on the page and extract the outlinks from it
	textOutlinks := extractLinksFromText(doc.Find("body").RemoveFiltered("script").Text())
	outlinks = append(outlinks, textOutlinks...)

	// Go over all outlinks and make sure they are absolute links
	outlinks = utils.MakeAbsolute(base, outlinks)

	// Hash (or fragment) URLs are navigational links pointing to the exact same page as such, they should not be treated as new outlinks.
	outlinks = utils.RemoveFragments(outlinks)

	return utils.DedupeURLs(outlinks), nil
}

func queueOutlinks(outlinks []*url.URL, sourceItem *item.Item, wg *sync.WaitGroup) {
	defer wg.Done()

	// Send the outlinks to the pool of workers
	for _, outlink := range outlinks {
		var newItem *item.Item
		var err error
		if packageClient.domainsCrawl && strings.Contains(sourceItem.URL.Host, outlink.Host) && sourceItem.Hop == 0 {
			newItem, err = item.New(outlink, sourceItem.URL, item.TypeSeed, 0, "", false)
		} else if packageClient.maxHops >= sourceItem.Hop+1 {
			newItem, err = item.New(outlink, sourceItem.URL, item.TypeOutlink, sourceItem.Hop+1, "", false)
		}
		if err != nil {
			packageClient.logger.Error("unable to create new item from outlink, discarding", "error", err, "outlink", outlink.String())
			continue
		}
		reactor.Recv(newItem)
	}
}
