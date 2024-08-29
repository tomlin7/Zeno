package upload

import (
	"fmt"
	"path/filepath"
	"strings"
)

func (c *Config) generateItemName(filePath string) string {
	parts := strings.Split(strings.TrimSuffix(filepath.Base(filePath), ".warc.gz"), "-")
	return fmt.Sprintf("%s-%s-%s", parts[0], parts[1], parts[3])
}
