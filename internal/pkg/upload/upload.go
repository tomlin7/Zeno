package upload

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/rclone/rclone/backend/internetarchive"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/operations"
)

func (c *Config) Start() {
	// Set up rclone config
	config.ClearConfigPassword()
	config.SetConfigPath(":memory:")
	config.FileSet("ia", "type", "internetarchive")
	config.FileSet("ia", "access_key_id", c.IAAccessKey)
	config.FileSet("ia", "secret_access_key", c.IASecretKey)
	go c.continuousUpload()
}

func (c *Config) continuousUpload() {
	var (
		itemName string
		itemSize int64
	)

	for {
		select {
		case <-c.DoneChan:
			log.Println("Received stop signal.")
			return
		default:
			// Process files in WARCsDir
			filepath.Walk(c.WARCsDir, func(filePath string, info os.FileInfo, err error) error {
				if err != nil {
					log.Printf("Error accessing path %q: %v\n", filePath, err)
					return nil
				}

				if info.IsDir() || !strings.HasSuffix(strings.ToLower(info.Name()), ".warc.gz") {
					return nil
				}

				if itemName == "" || itemSize > c.IAItemSize {
					itemName = c.generateItemName(filePath)
					itemSize = 0
				}

				// Upload the file
				err = c.uploadFile(itemName, filePath)
				if err != nil {
					log.Printf("Error uploading file %s: %v", filePath, err)
					return err
				}

				log.Printf("Successfully uploaded %s to item %s", filePath, itemName)

				itemSize += info.Size()

				return nil
			})

			// Sleep for a short duration before the next iteration
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *Config) uploadFile(itemName, filePath string) error {
	ctx := context.Background()

	srcFs, err := fs.NewFs(ctx, filepath.Dir(filePath))
	if err != nil {
		return fmt.Errorf("failed to create source fs: %v", err)
	}

	fileName := filepath.Base(filePath)

	// Create destination path with metadata
	dstFs, err := fs.NewFs(ctx, "ia:"+itemName)
	if err != nil {
		return fmt.Errorf("failed to create destination fs: %v", err)
	}

	dstFs.Features()

	// Perform the file copy
	err = operations.MoveFile(ctx, dstFs, srcFs, fileName, fileName)
	if err != nil {
		return fmt.Errorf("failed to upload file: %v", err)
	}

	return nil
}
