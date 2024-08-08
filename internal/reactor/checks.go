package reactor

import (
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/utils"
)

func (p *reactor) checkHost(item *item.Item) bool {
	// Check if host included, if includedHosts is empty, all hosts are included
	if len(p.includedHosts) > 0 && !utils.StringInSlice(item.URL.Host, p.includedHosts) {
		return false
	}

	// Check if host excluded
	if utils.StringInSlice(item.URL.Host, p.excludedHosts) {
		return false
	}

	return true
}
