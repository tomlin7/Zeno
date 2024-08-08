package item

import (
	"hash/fnv"
	"net/url"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/utils"
)

type Item struct {
	URL             *url.URL
	ParentURL       *url.URL
	Hop             uint64
	Type            string
	ID              string
	BypassSeencheck bool
	Hash            uint64
	LocallyCrawled  uint64
	Redirect        uint64
}

func New(URL *url.URL, parentURL *url.URL, itemType string, hop uint64, ID string, bypassSeencheck bool) (*Item, error) {
	h := fnv.New64a()
	h.Write([]byte(utils.URLToString(URL)))

	if ID == "" {
		ID = uuid.New().String()
	}

	return &Item{
		URL:             URL,
		ParentURL:       parentURL,
		Hop:             hop,
		Type:            itemType,
		ID:              ID,
		Hash:            h.Sum64(),
		BypassSeencheck: bypassSeencheck,
	}, nil
}
