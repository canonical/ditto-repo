package main

import (
	"context"
	"cmp"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/canonical/ditto-repo/repo"
)

const (
	configFileName = "ditto-config.json"

	// Environment variable names
	configPathEnv   = "DITTO_CONFIG_PATH"
	repoURLEnv      = "DITTO_REPO_URL"
	distEnv         = "DITTO_DIST"
	distsEnv        = "DITTO_DISTS"
	componentsEnv   = "DITTO_COMPONENTS"
	archsEnv        = "DITTO_ARCHS"
	languagesEnv    = "DITTO_LANGUAGES"
	downloadPathEnv = "DITTO_DOWNLOAD_PATH"
	workersEnv      = "DITTO_WORKERS"

	// Flag names and descriptions
	configPath                  = "config"
	configPathDescription       = "Path to config file (overrides ditto-config.json if exists)"
	repoURLFlag                 = "repo-url"
	repoURLFlagDescription      = "Repository URL"
	distFlag                    = "dist"
	distFlagDescription         = "Distribution (deprecated, use dists)"
	distsFlag                   = "dists"
	distsFlagDescription        = "Distributions (comma-separated)"
	componentsFlag              = "components"
	componentsFlagDescription   = "Components (comma-separated)"
	archsFlag                   = "archs"
	archsFlagDescription        = "Architectures (comma-separated)"
	languagesFlag               = "languages"
	languagesFlagDescription    = "Languages (comma-separated)"
	downloadPathFlag            = "download-path"
	downloadPathFlagDescription = "Download path"
	workersFlag                 = "workers"
	workersFlagDescription      = "Number of workers"
)

//go:embed config.default.json
var defaultConfig []byte

func main() {
	// Define CLI flags
	var (
		flagConfigPath   = flag.String(configPath, "", configPathDescription)
		flagRepoURL      = flag.String(repoURLFlag, "", repoURLFlagDescription)
		flagDist         = flag.String(distFlag, "", distFlagDescription)
		flagDists        = flag.String(distsFlag, "", distsFlagDescription)
		flagComponents   = flag.String(componentsFlag, "", componentsFlagDescription)
		flagArchs        = flag.String(archsFlag, "", archsFlagDescription)
		flagLanguages    = flag.String(languagesFlag, "", languagesFlagDescription)
		flagDownloadPath = flag.String(downloadPathFlag, "", downloadPathFlagDescription)
		flagWorkers      = flag.Int(workersFlag, 0, workersFlagDescription)
	)
	flag.Parse()

	var err error

	var configData []byte
	// Override configPath with command-line arg or environment variable if provided
	// First check if config path is provided via CLI flag. That avoids issues
	// with users forggeting about variables they set.
	// Otherwise try to read ditto-config.json from current directory by deault
	// the historically default behavior is to read from ditto-config.json if it exists.
	// Otherwise use embedded default config.
	var configPath = cmp.Or(*flagConfigPath, os.Getenv(configPathEnv))
	if configPath != "" {
		configData, err = os.ReadFile(configPath)
		if err != nil {
			log.Fatalf("Failed to read config from %s: %v", *flagConfigPath, err)
		}
	} else {
		log.Println("No config provided via env or param. Fallback to the embedded config")
		configData, err = os.ReadFile(configFileName)
		if err != nil {
			// File doesn't exist, use embedded default config
			configData = defaultConfig
		}
	}

	var config repo.DittoConfig
	err = json.Unmarshal(configData, &config)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}


	if repoURL := os.Getenv(repoURLEnv); repoURL != "" {
		config.RepoURL = repoURL
	}
	if dist := os.Getenv(distEnv); dist != "" {
		config.Dist = dist
	}
	if dists := os.Getenv(distsEnv); dists != "" {
		config.Dists = strings.Split(dists, ",")
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
	if workers := os.Getenv(workersEnv); workers != "" {
		var w int
		_, err := fmt.Sscanf(workers, "%d", &w)
		if err == nil {
			config.Workers = w
		}
	}

	// Override config with CLI flags if set
	if *flagRepoURL != "" {
		config.RepoURL = *flagRepoURL
	}
	if *flagDist != "" {
		config.Dist = *flagDist
	}
	if *flagDists != "" {
		config.Dists = strings.Split(*flagDists, ",")
	}
	if *flagComponents != "" {
		config.Components = strings.Split(*flagComponents, ",")
	}
	if *flagArchs != "" {
		config.Archs = strings.Split(*flagArchs, ",")
	}
	if *flagLanguages != "" {
		config.Languages = strings.Split(*flagLanguages, ",")
	}
	if *flagDownloadPath != "" {
		config.DownloadPath = *flagDownloadPath
	}
	if *flagWorkers > 0 {
		config.Workers = *flagWorkers
	}

	d := repo.NewDittoRepo(config)

	// Create a context with cancellation support
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown on interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received interrupt signal, cancelling mirror...")
		cancel()
	}()

	// Start the mirror and get progress channel
	progressChan := d.Mirror(ctx)

	// Monitor progress
	lastUpdate := time.Now()
	for update := range progressChan {
		// Print progress updates every second to avoid console spam
		if time.Since(lastUpdate) >= time.Second {
			log.Printf("Progress: %d/%d packages downloaded (Current: %s)",
				update.PackagesDownloaded, update.TotalPackages, update.CurrentFile)
			lastUpdate = time.Now()
		}
	}

	log.Println("Mirror complete!")
}
