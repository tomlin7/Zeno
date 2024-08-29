package upload

type Config struct {
	IACollections   []string
	IAMediatype     string
	IAItemSize      int64
	IAAccessKey     string
	IASecretKey     string
	DoneChan        chan bool
	WARCsDir        string
	ParallelUploads int
}

func NewConfig(WARCsDir string) *Config {
	return &Config{
		WARCsDir:        WARCsDir,
		ParallelUploads: 4, // Default to 4 parallel uploads
		DoneChan:        make(chan bool),
	}
}
