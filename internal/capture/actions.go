package capture

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/clbanning/mxj/v2"
	"github.com/internetarchive/Zeno/internal/capture/sitespecific/cloudflarestream"
	"github.com/internetarchive/Zeno/internal/capture/sitespecific/facebook"
	"github.com/internetarchive/Zeno/internal/capture/sitespecific/libsyn"
	"github.com/internetarchive/Zeno/internal/capture/sitespecific/telegram"
	"github.com/internetarchive/Zeno/internal/capture/sitespecific/tiktok"
	"github.com/internetarchive/Zeno/internal/capture/sitespecific/truthsocial"
	"github.com/internetarchive/Zeno/internal/capture/sitespecific/vk"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/queue"
	"github.com/internetarchive/Zeno/internal/stats"
	"github.com/internetarchive/Zeno/internal/utils"
	"github.com/remeh/sizedwaitgroup"
)

// Capture capture the URL and return the outlinks
func Capture(itemToCapture *item.Item) error {
	var (
		resp      *http.Response
		waitGroup sync.WaitGroup
	)

	defer func(i *item.Item) {
		waitGroup.Wait()

		if packageClient.useHQ && i.ID != "" {
			packageClient.hqFinishedChannel <- i
		}
	}(itemToCapture)

	// Prepare GET request
	req, err := http.NewRequest("GET", utils.URLToString(itemToCapture.URL), nil)
	if err != nil {
		packageClient.logger.Error("error while preparing GET request", "error", err, "url", itemToCapture.URL)
		return err
	}

	if itemToCapture.Hop > 0 && itemToCapture.ParentURL != nil {
		req.Header.Set("Referer", utils.URLToString(itemToCapture.ParentURL))
	}

	req.Header.Set("User-Agent", packageClient.userAgent)

	// Execute site-specific code on the request, before sending it
	if truthsocial.IsTruthSocialURL(utils.URLToString(itemToCapture.URL)) {
		// Get the API URL from the URL
		APIURL, err := truthsocial.GenerateAPIURL(utils.URLToString(itemToCapture.URL))
		if err != nil {
			packageClient.logger.Error("error while generating API URL", "error", err, "url", itemToCapture.URL)
		} else {
			if APIURL == nil {
				packageClient.logger.Error("error while generating API URL", "error", err, "url", itemToCapture.URL)
			} else {
				// Then we create an item
				APIItem, err := item.New(APIURL, itemToCapture.URL, itemToCapture.Type, itemToCapture.Hop, itemToCapture.ID, false)
				if err != nil {
					packageClient.logger.Error("error while creating TruthSocial API item", "error", err, "url", itemToCapture.URL)
				} else {
					err = Capture(APIItem)
					if err != nil {
						packageClient.logger.Error("error while capturing TruthSocial API URL", "error", err, "url", itemToCapture.URL)
					}
				}
			}

			// Grab few embeds that are needed for the playback
			embedURLs, err := truthsocial.EmbedURLs()
			if err != nil {
				packageClient.logger.Error("error while getting TruthSocial embed URLs", "error", err, "url", itemToCapture.URL)
			} else {
				for _, embedURL := range embedURLs {
					// Create the embed item
					embedItem, err := item.New(embedURL, itemToCapture.URL, itemToCapture.Type, itemToCapture.Hop, itemToCapture.ID, false)
					if err != nil {
						packageClient.logger.Error("error while creating TruthSocial embed item", "error", err, "url", itemToCapture.URL)
					} else {
						err = Capture(embedItem)
						if err != nil {
							packageClient.logger.Error("error while capturing TruthSocial embed URL", "error", err, "url", itemToCapture.URL)
						}
					}
				}
			}
		}
	} else if facebook.IsFacebookPostURL(utils.URLToString(itemToCapture.URL)) {
		// Generate the embed URL
		embedURL, err := facebook.GenerateEmbedURL(utils.URLToString(itemToCapture.URL))
		if err != nil {
			packageClient.logger.Error("error while generating Facebook embed URL", "error", err, "url", itemToCapture.URL)
		} else {
			if embedURL == nil {
				packageClient.logger.Error("error while generating Facebook embed URL", "error", err, "url", itemToCapture.URL)
			} else {
				// Create the embed item
				embedItem, err := item.New(embedURL, itemToCapture.URL, itemToCapture.Type, itemToCapture.Hop, itemToCapture.ID, false)
				if err != nil {
					packageClient.logger.Error("error while creating Facebook embed item", "error", err, "url", itemToCapture.URL)
				} else {
					err = Capture(embedItem)
					if err != nil {
						packageClient.logger.Error("error while capturing Facebook embed URL", "error", err, "url", itemToCapture.URL)
					}
				}
			}
		}
	} else if libsyn.IsLibsynURL(utils.URLToString(itemToCapture.URL)) {
		// Generate the highwinds URL
		highwindsURL, err := libsyn.GenerateHighwindsURL(utils.URLToString(itemToCapture.URL))
		if err != nil {
			packageClient.logger.Error("error while generating libsyn URL", "error", err, "url", itemToCapture.URL)
		} else {
			if highwindsURL == nil {
				packageClient.logger.Error("error while generating libsyn URL", "error", err, "url", itemToCapture.URL)
			} else {
				highwindsItem, err := item.New(highwindsURL, itemToCapture.URL, itemToCapture.Type, itemToCapture.Hop, itemToCapture.ID, false)
				if err != nil {
					packageClient.logger.Error("error while creating libsyn highwinds item", "error", err, "url", itemToCapture.URL)
				} else {
					err = Capture(highwindsItem)
					if err != nil {
						packageClient.logger.Error("error while capturing libsyn highwinds URL", "error", err, "url", itemToCapture.URL)
					}
				}
			}
		}
	} else if tiktok.IsTikTokURL(utils.URLToString(itemToCapture.URL)) {
		tiktok.AddHeaders(req)
	} else if telegram.IsTelegramURL(utils.URLToString(itemToCapture.URL)) && !telegram.IsTelegramEmbedURL(utils.URLToString(itemToCapture.URL)) {
		// If the URL is a Telegram URL, we make an embed URL out of it
		telegram.TransformURL(itemToCapture.URL)

		// Then we create an item
		embedItem, err := item.New(itemToCapture.URL, itemToCapture.URL, itemToCapture.Type, itemToCapture.Hop, itemToCapture.ID, false)
		if err != nil {
			packageClient.logger.Error("error while creating Telegram embed item", "error", err, "url", itemToCapture.URL)
		} else {
			// And capture it
			err = Capture(embedItem)
			if err != nil {
				// c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while capturing Telegram embed URL")
				packageClient.logger.Error("error while capturing Telegram embed URL", "error", err, "url", itemToCapture.URL)
			}
		}
	} else if vk.IsVKURL(utils.URLToString(itemToCapture.URL)) {
		vk.AddHeaders(req)
	}

	// Execute request
	resp, err = executeGET(itemToCapture, req, false)
	if err != nil && err.Error() == "URL from redirection has already been seen" {
		return err
	} else if err != nil && err.Error() == "URL is being rate limited, sending back to HQ" {
		newItem, err := item.New(itemToCapture.URL, itemToCapture.ParentURL, itemToCapture.Type, itemToCapture.Hop, "", true)
		if err != nil {
			packageClient.logger.Error("error while creating new item", "error", err, "url", itemToCapture.URL)
			return err
		}

		packageClient.hqProducerChannel <- newItem
		packageClient.logger.Info("URL is being rate limited, sent back to HQ, skipping reactor", "error", err, "url", itemToCapture.URL)
		return err
	} else if err != nil {
		packageClient.logger.Error("error while executing GET request", "error", err, "url", itemToCapture.URL)
		return err
	}
	defer resp.Body.Close()

	// Scrape potential URLs from Link HTTP header
	var (
		links      = Parse(resp.Header.Get("link"))
		discovered []string
	)

	for _, link := range links {
		discovered = append(discovered, link.URL)
	}

	waitGroup.Add(1)
	go queueOutlinks(utils.MakeAbsolute(itemToCapture.URL, utils.StringSliceToURLSlice(discovered)), itemToCapture, &waitGroup)

	// Store the base URL to turn relative links into absolute links later
	base, err := url.Parse(utils.URLToString(resp.Request.URL))
	if err != nil {
		packageClient.logger.Error("error while parsing base URL", "error", err, "url", itemToCapture.URL)
		return err
	}

	// If the response is a JSON document, we want to scrape it for links
	if strings.Contains(resp.Header.Get("Content-Type"), "json") {
		jsonBody, err := io.ReadAll(resp.Body)
		if err != nil {
			packageClient.logger.Error("error while reading JSON body", "error", err, "url", itemToCapture.URL)
			return err
		}

		outlinksFromJSON, err := getURLsFromJSON(string(jsonBody))
		if err != nil {
			packageClient.logger.Error("error while getting URLs from JSON", "error", err, "url", itemToCapture.URL)
			return err
		}

		waitGroup.Add(1)
		go queueOutlinks(utils.MakeAbsolute(itemToCapture.URL, utils.StringSliceToURLSlice(outlinksFromJSON)), itemToCapture, &waitGroup)

		return err
	}

	// If the response is an XML document, we want to scrape it for links
	if strings.Contains(resp.Header.Get("Content-Type"), "xml") {
		xmlBody, err := io.ReadAll(resp.Body)
		if err != nil {
			packageClient.logger.Error("error while reading XML body", "error", err, "url", itemToCapture.URL)
			return err
		}

		mv, err := mxj.NewMapXml(xmlBody)
		if err != nil {
			packageClient.logger.Error("error while parsing XML body", "error", err, "url", itemToCapture.URL)
			return err
		}

		for _, value := range mv.LeafValues() {
			if _, ok := value.(string); ok {
				if strings.HasPrefix(value.(string), "http") {
					discovered = append(discovered, value.(string))
				}
			}
		}
	}

	// If the response isn't a text/*, we do not scrape it.
	// We also aren't going to scrape if assets and outlinks are turned off.
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/") || (packageClient.disableAssetsCapture && !packageClient.domainsCrawl && (uint64(packageClient.maxHops) <= itemToCapture.Hop)) {
		// Enforce reading all data from the response for WARC writing
		_, err := io.Copy(io.Discard, resp.Body)
		if err != nil {
			packageClient.logger.Error("error while reading response body", "error", err, "url", itemToCapture.URL)
		}

		return err
	}

	// Turn the response into a doc that we will scrape for outlinks and assets.
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		packageClient.logger.Error("error while creating goquery document", "error", err, "url", itemToCapture.URL)
		return err
	}

	// Execute site-specific code on the document
	if strings.Contains(base.Host, "cloudflarestream.com") {
		// Look for JS files necessary for the playback of the video
		cfstreamURLs, err := cloudflarestream.GetJSFiles(doc, base, *packageClient.client)
		if err != nil {
			packageClient.logger.Error("error while getting JS files from cloudflarestream", "error", err, "url", itemToCapture.URL)
			return err
		}

		// Seencheck the URLs we captured, we ignore the returned value here
		// because we already archived the URLs, we just want them to be added
		// to the seencheck table.
		if packageClient.useSeencheck {
			for _, cfstreamURL := range cfstreamURLs {
				packageClient.seencheckURL(cfstreamURL, "asset")
			}
		} else if packageClient.useHQ {
			_, err := c.HQSeencheckURLs(utils.StringSliceToURLSlice(cfstreamURLs))
			if err != nil {
				packageClient.logger.Error("error while seenchecking Cloudflarestream assets via HQ", "error", err, "url", itemToCapture.URL)
			}
		}

		// Log the archived URLs
		for _, cfstreamURL := range cfstreamURLs {
			packageClient.logger.Info("URL archived", "url", cfstreamURL, "parentHop", itemToCapture.Hop, "parentURL", itemToCapture.URL)
		}
	}

	// Websites can use a <base> tag to specify a base for relative URLs in every other tags.
	// This checks for the "base" tag and resets the "base" URL variable with the new base URL specified
	// https://developer.mozilla.org/en-US/docs/Web/HTML/Element/base
	if !utils.StringInSlice("base", c.DisabledHTMLTags) {
		oldBase := base

		doc.Find("base").Each(func(index int, goitem *goquery.Selection) {
			// If a new base got scraped, stop looking for one
			if oldBase != base {
				return
			}

			// Attempt to get a new base value from the base HTML tag
			link, exists := goitem.Attr("href")
			if exists {
				baseTagValue, err := url.Parse(link)
				if err != nil {
					c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while parsing base tag value")
				} else {
					base = baseTagValue
				}
			}
		})
	}

	// Extract outlinks
	outlinks, err := extractOutlinks(base, doc)
	if err != nil {
		c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while extracting outlinks")
		return err
	}

	waitGroup.Add(1)
	go c.queueOutlinks(outlinks, item, &waitGroup)

	if c.DisableAssetsCapture {
		return err
	}

	// Extract and capture assets
	assets, err := c.extractAssets(base, item, doc)
	if err != nil {
		c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while extracting assets")
		return err
	}

	// If we didn't find any assets, let's stop here
	if len(assets) == 0 {
		return err
	}

	// If --local-seencheck is enabled, then we check if the assets are in the
	// seencheck DB. If they are, then they are skipped.
	// Else, if we use HQ, then we use HQ's seencheck.
	if c.UseSeencheck {
		seencheckedBatch := []*url.URL{}

		for _, URL := range assets {
			found := c.seencheckURL(utils.URLToString(URL), "asset")
			if found {
				continue
			}
			seencheckedBatch = append(seencheckedBatch, URL)
		}

		if len(seencheckedBatch) == 0 {
			return err
		}

		assets = seencheckedBatch
	} else if c.UseHQ {
		seencheckedURLs, err := c.HQSeencheckURLs(assets)
		// We ignore the error here because we don't want to slow down the crawl
		// if HQ is down or if the request failed. So if we get an error, we just
		// continue with the original list of assets.
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{
				"urls":      assets,
				"parentHop": item.Hop,
				"parentUrl": utils.URLToString(item.URL),
			})).Error("error while seenchecking assets via HQ")
		} else {
			assets = seencheckedURLs
		}

		if len(assets) == 0 {
			return err
		}
	}

	// TODO: implement a counter for the number of assets
	// currently being processed
	// c.Frontier.QueueCount.Incr(int64(len(assets)))
	swg := sizedwaitgroup.New(int(c.MaxConcurrentAssets))
	excluded := false

	for _, asset := range assets {
		// TODO: implement a counter for the number of assets
		// currently being processed
		// c.Frontier.QueueCount.Incr(-1)

		// Just making sure we do not over archive by archiving the original URL
		if utils.URLToString(item.URL) == utils.URLToString(asset) {
			continue
		}

		// We ban googlevideo.com URLs because they are heavily rate limited by default, and
		// we don't want the crawler to spend an innapropriate amount of time archiving them
		if strings.Contains(item.URL.Host, "googlevideo.com") {
			continue
		}

		// If the URL match any excluded string, we ignore it
		for _, excludedString := range c.ExcludedStrings {
			if strings.Contains(utils.URLToString(asset), excludedString) {
				excluded = true
				break
			}
		}

		if excluded {
			excluded = false
			continue
		}

		swg.Add()
		stats.IncreaseURIPerSecond(1)

		go func(asset *url.URL, swg *sizedwaitgroup.SizedWaitGroup) {
			defer swg.Done()

			// Create the asset's item
			newAsset, err := item.New(asset, item.URL, "asset", item.Hop, "", false)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, asset, map[string]interface{}{
					"parentHop": item.Hop,
					"parentUrl": utils.URLToString(item.URL),
					"type":      "asset",
				})).Error("error while creating asset item")
				return
			}

			// Capture the asset
			err = c.captureAsset(newAsset, resp.Cookies())
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, &asset, map[string]interface{}{
					"parentHop": item.Hop,
					"parentUrl": utils.URLToString(item.URL),
					"type":      "asset",
				})).Error("error while capturing asset")
				return
			}

			// If we made it to this point, it means that the asset have been crawled successfully,
			// then we can increment the locallyCrawled variable
			atomic.AddUint64(&item.LocallyCrawled, 1)
		}(asset, &swg)
	}

	swg.Wait()
	return err
}

func executeGET(item *queue.Item, req *http.Request, isRedirection bool) (resp *http.Response, err error) {
	var (
		executionStart = time.Now()
		newItem        *queue.Item
		newReq         *http.Request
		URL            *url.URL
	)

	defer func() {
		if c.PrometheusMetrics != nil {
			c.PrometheusMetrics.DownloadedURI.Inc()
		}

		stats.IncreaseURIPerSecond(1)
		if item.Type == "seed" {
			stats.IncreaseCrawledSeeds(1)
		} else if item.Type == "asset" {
			stats.IncreaseCrawledAssets(1)
		}
	}()

	// Check if the crawl is paused
	for c.Paused.Get() {
		time.Sleep(time.Second)
	}

	// TODO: re-implement host limitation
	// Temporarily pause crawls for individual hosts if they are over our configured maximum concurrent requests per domain.
	// If the request is a redirection, we do not pause the crawl because we want to follow the redirection.
	// if !isRedirection {
	// for c.shouldPause(item.Host) {
	// 	time.Sleep(time.Millisecond * time.Duration(c.RateLimitDelay))
	// }

	// c.Queue.IncrHostActive(item.Host)
	// defer c.Frontier.DecrHostActive(item.Host)
	//}

	// Retry on 429 error
	for retry := uint8(0); retry < c.MaxRetry; retry++ {
		// Execute GET request
		if c.ClientProxied == nil || utils.StringContainsSliceElements(req.URL.Host, c.BypassProxy) {
			resp, err = c.Client.Do(req)
			if err != nil {
				if retry+1 >= c.MaxRetry {
					return resp, err
				}
			}
		} else {
			resp, err = c.ClientProxied.Do(req)
			if err != nil {
				if retry+1 >= c.MaxRetry {
					return resp, err
				}
			}
		}

		// This is unused unless there is an error or a 429.
		sleepTime := time.Second * time.Duration(retry*2) // Retry after 0s, 2s, 4s, ... this could be tweaked in the future to be more customizable.

		if err != nil {
			if strings.Contains(err.Error(), "unsupported protocol scheme") || strings.Contains(err.Error(), "no such host") {
				return nil, err
			}

			c.Log.WithFields(c.genLogFields(err, req.URL, nil)).Error("error while executing GET request, retrying", "retries", retry)

			time.Sleep(sleepTime)

			continue
		}

		if resp.StatusCode == 429 {
			c.Log.WithFields(c.genLogFields(err, req.URL, map[string]interface{}{
				"sleepTime":  sleepTime.String(),
				"retryCount": retry,
				"statusCode": resp.StatusCode,
			})).Info("we are being rate limited")

			// This ensures we aren't leaving the warc dialer hanging.
			// Do note, 429s are filtered out by WARC writer regardless.
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			// If --hq-rate-limiting-send-back is enabled, we send the URL back to HQ
			if c.UseHQ && c.HQRateLimitingSendBack {
				return nil, errors.New("URL is being rate limited, sending back to HQ")
			}
			c.Log.WithFields(c.genLogFields(err, req.URL, map[string]interface{}{
				"sleepTime":  sleepTime.String(),
				"retryCount": retry,
				"statusCode": resp.StatusCode,
			})).Warn("URL is being rate limited")

			continue
		}
		c.logCrawlSuccess(executionStart, resp.StatusCode, item)
		break
	}

	// If a redirection is catched, then we execute the redirection
	if isStatusCodeRedirect(resp.StatusCode) {
		if resp.Header.Get("location") == utils.URLToString(req.URL) || item.Redirect >= uint64(c.MaxRedirect) {
			return resp, nil
		}
		defer resp.Body.Close()

		// Needed for WARC writing
		// IMPORTANT! This will write redirects to WARC!
		io.Copy(io.Discard, resp.Body)

		URL, err = url.Parse(resp.Header.Get("location"))
		if err != nil {
			return resp, err
		}

		// Make URL absolute if they aren't.
		// Some redirects don't return full URLs, but rather, relative URLs. We would still like to follow these redirects.
		if !URL.IsAbs() {
			URL = req.URL.ResolveReference(URL)
		}

		// Seencheck the URL
		if c.UseSeencheck {
			found := c.seencheckURL(utils.URLToString(URL), "seed")
			if found {
				return nil, errors.New("URL from redirection has already been seen")
			}
		} else if c.UseHQ {
			isNewURL, err := c.HQSeencheckURL(URL)
			if err != nil {
				return resp, err
			}

			if !isNewURL {
				return nil, errors.New("URL from redirection has already been seen")
			}
		}

		newItem, err = item.New(URL, item.URL, item.Type, item.Hop, item.ID, false)
		if err != nil {
			return nil, err
		}

		newItem.Redirect = item.Redirect + 1

		// Prepare GET request
		newReq, err = http.NewRequest("GET", utils.URLToString(URL), nil)
		if err != nil {
			return nil, err
		}

		// Set new request headers on the new request :(
		newReq.Header.Set("User-Agent", c.UserAgent)
		newReq.Header.Set("Referer", utils.URLToString(newItem.ParentURL))

		return c.executeGET(newItem, newReq, true)
	}

	return resp, nil
}

func captureAsset(item *item.Item, cookies []*http.Cookie) error {
	var resp *http.Response

	// Prepare GET request
	req, err := http.NewRequest("GET", utils.URLToString(item.URL), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Referer", utils.URLToString(item.ParentURL))
	req.Header.Set("User-Agent", c.UserAgent)

	// Apply cookies obtained from the original URL captured
	for i := range cookies {
		req.AddCookie(cookies[i])
	}

	resp, err = c.executeGET(item, req, false)
	if err != nil && err.Error() == "URL from redirection has already been seen" {
		return nil
	} else if err != nil {
		return err
	}
	defer resp.Body.Close()

	// needed for WARC writing
	io.Copy(io.Discard, resp.Body)

	return nil
}

func getURLsFromJSON(jsonString string) ([]string, error) {
	var data interface{}
	err := json.Unmarshal([]byte(jsonString), &data)
	if err != nil {
		return nil, err
	}

	links := make([]string, 0)
	findURLs(data, &links)

	return links, nil
}

func findURLs(data interface{}, links *[]string) {
	switch v := data.(type) {
	case string:
		if isValidURL(v) {
			*links = append(*links, v)
		}
	case []interface{}:
		for _, element := range v {
			findURLs(element, links)
		}
	case map[string]interface{}:
		for _, value := range v {
			findURLs(value, links)
		}
	}
}

func isValidURL(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}