package main

import (
	"cmp"
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
	configPathEnv          = "DITTO_CONFIG_PATH"
	repoURLEnv             = "DITTO_REPO_URL"
	repoURLsEnv            = "DITTO_REPO_URLS"
	archURLsEnv            = "DITTO_ARCH_URLS"
	distEnv                = "DITTO_DIST"
	distsEnv               = "DITTO_DISTS"
	componentsEnv          = "DITTO_COMPONENTS"
	archsEnv               = "DITTO_ARCHS"
	languagesEnv           = "DITTO_LANGUAGES"
	downloadPathEnv        = "DITTO_DOWNLOAD_PATH"
	workersEnv             = "DITTO_WORKERS"
	debugEnv               = "DITTO_DEBUG"
	verifyModeEnv          = "DITTO_VERIFY_MODE"
	allowMissingIndicesEnv = "DITTO_ALLOW_MISSING_INDICES"

	// Flag names and descriptions
	configPath                         = "config"
	configPathDescription              = "Path to config file (default: ./ditto-config.json)"
	repoURLFlag                        = "repo-url"
	repoURLFlagDescription             = "Repository URL (deprecated, use repo-urls)"
	repoURLsFlag                       = "repo-urls"
	repoURLsFlagDescription            = "Repository URLs (comma-separated mirrors serving identical Release files)"
	archURLsFlag                       = "arch-urls"
	archURLsFlagDescription            = "Per-architecture mirror preferences (comma-separated arch=url pairs)"
	distFlag                           = "dist"
	distFlagDescription                = "Distribution (deprecated, use dists)"
	distsFlag                          = "dists"
	distsFlagDescription               = "Distributions (comma-separated)"
	componentsFlag                     = "components"
	componentsFlagDescription          = "Components (comma-separated)"
	archsFlag                          = "archs"
	archsFlagDescription               = "Architectures (comma-separated)"
	languagesFlag                      = "languages"
	languagesFlagDescription           = "Languages (comma-separated)"
	downloadPathFlag                   = "download-path"
	downloadPathFlagDescription        = "Download path"
	workersFlag                        = "workers"
	workersFlagDescription             = "Number of workers"
	verifyModeFlag                     = "verify-mode"
	verifyModeFlagDescription          = "File verification mode: checksum (default) or size"
	allowMissingIndicesFlag            = "allow-missing-indices"
	allowMissingIndicesFlagDescription = "Warn instead of failing when a Packages index file cannot be fetched"
	debugFlag                          = "debug"
	debugFlagDescription               = "Enable debug logging"
)

//go:embed config.default.json
var defaultConfig []byte

// parseArchURLs parses a comma-separated list of "arch=url" pairs into a map suitable for
// DittoConfig.ArchURLs (e.g. "arm64=https://ports.ubuntu.com/ubuntu,armhf=https://ports.ubuntu.com/ubuntu").
// Entries lacking an "=" separator or with an empty architecture or URL are skipped.
// Returns nil when no valid pairs are present.
func parseArchURLs(s string) map[string]string {
	result := make(map[string]string)
	for _, pair := range strings.Split(s, ",") {
		arch, url, found := strings.Cut(pair, "=")
		arch = strings.TrimSpace(arch)
		url = strings.TrimSpace(url)
		if !found || arch == "" || url == "" {
			continue
		}
		result[arch] = url
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func main() {
	// Define CLI flags
	var (
		flagConfigPath          = flag.String(configPath, "", configPathDescription)
		flagRepoURL             = flag.String(repoURLFlag, "", repoURLFlagDescription)
		flagRepoURLs            = flag.String(repoURLsFlag, "", repoURLsFlagDescription)
		flagArchURLs            = flag.String(archURLsFlag, "", archURLsFlagDescription)
		flagDist                = flag.String(distFlag, "", distFlagDescription)
		flagDists               = flag.String(distsFlag, "", distsFlagDescription)
		flagComponents          = flag.String(componentsFlag, "", componentsFlagDescription)
		flagArchs               = flag.String(archsFlag, "", archsFlagDescription)
		flagLanguages           = flag.String(languagesFlag, "", languagesFlagDescription)
		flagDownloadPath        = flag.String(downloadPathFlag, "", downloadPathFlagDescription)
		flagWorkers             = flag.Int(workersFlag, 0, workersFlagDescription)
		flagVerifyMode          = flag.String(verifyModeFlag, "", verifyModeFlagDescription)
		flagAllowMissingIndices = flag.Bool(allowMissingIndicesFlag, false, allowMissingIndicesFlagDescription)
		flagDebug               = flag.Bool(debugFlag, false, debugFlagDescription)
	)
	flag.Parse()

	var err error

	var configData []byte
	// Override configPath with command-line arg or environment variable if provided
	// First check if config path is provided via CLI flag. That avoids issues
	// with users forgetting about variables they set.
	// Otherwise try to read ditto-config.json from current directory by default
	// the historically default behavior is to read from ditto-config.json if it exists.
	// Otherwise use embedded default config.
	configPath := cmp.Or(*flagConfigPath, os.Getenv(configPathEnv))
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
	if repoURLs := os.Getenv(repoURLsEnv); repoURLs != "" {
		config.RepoURLs = strings.Split(repoURLs, ",")
	}
	if archURLs := os.Getenv(archURLsEnv); archURLs != "" {
		config.ArchURLs = parseArchURLs(archURLs)
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
	if verifyMode := os.Getenv(verifyModeEnv); verifyMode != "" {
		config.VerifyMode = repo.VerifyMode(verifyMode)
	}
	allowMissingVal := strings.ToLower(os.Getenv(allowMissingIndicesEnv))
	if allowMissingVal == "true" || allowMissingVal == "yes" || allowMissingVal == "1" {
		config.AllowMissingIndices = true
	}

	// Override config with CLI flags if set
	if *flagRepoURL != "" {
		config.RepoURL = *flagRepoURL
	}
	if *flagRepoURLs != "" {
		config.RepoURLs = strings.Split(*flagRepoURLs, ",")
	}
	if *flagArchURLs != "" {
		config.ArchURLs = parseArchURLs(*flagArchURLs)
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
	if *flagVerifyMode != "" {
		config.VerifyMode = repo.VerifyMode(*flagVerifyMode)
	}
	if *flagAllowMissingIndices {
		config.AllowMissingIndices = true
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
	progressChan, errChan := d.MirrorWithErrors(ctx)

	// Monitor progress
	lastUpdate := time.Now()
	var lastProgress repo.ProgressUpdate
	for update := range progressChan {
		lastProgress = update
		// Print progress updates every second to avoid console spam
		if time.Since(lastUpdate) >= time.Second {
			log.Printf("Progress: %d packages verified, %d packages downloaded, %d total packages (Current: %s)",
				update.PackagesVerified, update.PackagesDownloaded, update.TotalPackages, update.CurrentFile)
			lastUpdate = time.Now()
		}
	}
	log.Printf("Final: %d packages verified, %d packages downloaded, %d total packages (Last: %s)",
		lastProgress.PackagesVerified, lastProgress.PackagesDownloaded, lastProgress.TotalPackages, lastProgress.CurrentFile)

	// The error channel yields the terminal result once mirroring has finished.
	if err := <-errChan; err != nil {
		log.Fatalf("Mirror failed: %v", err)
	}

	log.Println("Mirror complete!")
}
