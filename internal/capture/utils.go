package capture

import (
	"hash/fnv"
	"strconv"
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
