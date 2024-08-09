package capture

import (
	"hash/fnv"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/utils"
)

func (c *client) seencheckURL(URL string, URLType string) bool {
	h := fnv.New64a()
	h.Write([]byte(URL))
	hash := strconv.FormatUint(h.Sum64(), 10)

	found, _ := c.seencheck.IsSeen(hash)
	if found {
		return true
	} else {
		c.seencheck.Seen(hash, URLType)
		return false
	}
}

var regexOutlinks *regexp.Regexp

func extractLinksFromText(source string) (links []*url.URL) {
	// Extract links and dedupe them
	rawLinks := utils.DedupeStrings(regexOutlinks.FindAllString(source, -1))

	// Validate links
	for _, link := range rawLinks {
		URL, err := url.Parse(link)
		if err != nil {
			continue
		}

		err = utils.ValidateURL(URL)
		if err != nil {
			continue
		}

		links = append(links, URL)
	}

	return links
}

func (c *client) logCrawlSuccess(executionStart time.Time, statusCode int, item *item.Item) {
	fields := make(map[string]interface{})
	fields["statusCode"] = statusCode
	fields["hop"] = item.Hop
	fields["type"] = item.Type
	fields["executionTime"] = time.Since(executionStart).Milliseconds()
	fields["url"] = utils.URLToString(item.URL)

	c.logger.Info("URL archived", fields)
}

func isStatusCodeRedirect(statusCode int) bool {
	if statusCode == 300 || statusCode == 301 ||
		statusCode == 302 || statusCode == 307 ||
		statusCode == 308 {
		return true
	}
	return false
}
