package main

import (
	_ "embed"
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/canonical/ditto-repo/repo"
)

const (
	configFileName  = "ditto-config.json"
	repoURLEnv      = "DITTO_REPO_URL"
	distEnv         = "DITTO_DIST"
	componentsEnv   = "DITTO_COMPONENTS"
	archsEnv        = "DITTO_ARCHS"
	languagesEnv    = "DITTO_LANGUAGES"
	downloadPathEnv = "DITTO_DOWNLOAD_PATH"
	workersEnv      = "DITTO_WORKERS"
)

//go:embed config.default.json
var defaultConfig []byte

func main() {
	var configData []byte

	// Try to read ditto-config.json from current directory
	data, err := os.ReadFile(configFileName)
	if err != nil {
		// File doesn't exist, use embedded default config
		configData = defaultConfig
	} else {
		configData = data
	}

	var config repo.DittoConfig
	err = json.Unmarshal(configData, &config)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	// Override config with environment variables if set
	if repoURL := os.Getenv(repoURLEnv); repoURL != "" {
		config.RepoURL = repoURL
	}
	if dist := os.Getenv(distEnv); dist != "" {
		config.Dist = dist
	}
	if components := os.Getenv(componentsEnv); components != "" {
		config.Components = strings.Split(components, ",")
	}
	if archs := os.Getenv(archsEnv); archs != "" {
		config.Archs = strings.Split(archs, ",")
	}
	if languages := os.Getenv(languagesEnv); languages != "" {
		config.Languages = strings.Split(languages, ",")
	}
	if downloadPath := os.Getenv(downloadPathEnv); downloadPath != "" {
		config.DownloadPath = downloadPath
	}

	d := repo.NewDittoRepo(config)
	err = d.Mirror()
	if err != nil {
		panic(err)
	}
}
