package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
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
	repoURLEnv      = "DITTO_REPO_URL"
	distEnv         = "DITTO_DIST"
	distsEnv        = "DITTO_DISTS"
	componentsEnv   = "DITTO_COMPONENTS"
	archsEnv        = "DITTO_ARCHS"
	languagesEnv    = "DITTO_LANGUAGES"
	downloadPathEnv = "DITTO_DOWNLOAD_PATH"
	workersEnv      = "DITTO_WORKERS"
	debugEnv        = "DITTO_DEBUG"

	// Flag names and descriptions
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
	debugFlag                   = "debug"
	debugFlagDescription        = "Enable debug logging"
)

//go:embed config.default.json
var defaultConfig []byte

func main() {
	// Define CLI flags
	var (
		flagRepoURL      = flag.String(repoURLFlag, "", repoURLFlagDescription)
		flagDist         = flag.String(distFlag, "", distFlagDescription)
		flagDists        = flag.String(distsFlag, "", distsFlagDescription)
		flagComponents   = flag.String(componentsFlag, "", componentsFlagDescription)
		flagArchs        = flag.String(archsFlag, "", archsFlagDescription)
		flagLanguages    = flag.String(languagesFlag, "", languagesFlagDescription)
		flagDownloadPath = flag.String(downloadPathFlag, "", downloadPathFlagDescription)
		flagWorkers      = flag.Int(workersFlag, 0, workersFlagDescription)
		flagDebug        = flag.Bool(debugFlag, false, debugFlagDescription)
	)
	flag.Parse()

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

	debugVal := strings.ToLower(os.Getenv(debugEnv))
	enableDebug := *flagDebug || (debugVal == "true" || debugVal == "yes" || debugVal == "1")
	if enableDebug {
		config.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
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
