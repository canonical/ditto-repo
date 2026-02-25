package repo

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

const (
	defaultWorkers = 5
)

// ProgressUpdate represents a progress event during mirroring
type ProgressUpdate struct {
	PackagesDownloaded int
	TotalPackages      int
	CurrentFile        string
}

// The canonical implementation of DittoRepo
type dittoRepo struct {
	config             DittoConfig
	logger             Logger
	fs                 FileSystem
	downloader         Downloader
	validPackages      map[string]bool // Track packages referenced in upstream
	mu                 sync.Mutex      // Protect validPackages map
	progressChan       chan ProgressUpdate
	packagesDownloaded int
	totalPackages      int
}

// DittoConfig holds all configuration for the mirroring process
type DittoConfig struct {
	RepoURL      string   `json:"repo-url"`
	Dist         string   `json:"dist"`  // Deprecated: use Dists instead
	Dists        []string `json:"dists"` // List of distributions to mirror
	Components   []string `json:"components"`
	Archs        []string `json:"archs"`
	Languages    []string `json:"languages"`     // Add languages here (e.g. "en", "es")
	DownloadPath string   `json:"download-path"` // Local storage root
	Workers      int      `json:"workers"`       // Number of concurrent download workers

	// Optional custom implementations
	Logger     Logger     `json:"-"`
	FileSystem FileSystem `json:"-"`
	Downloader Downloader `json:"-"`
}

func NewDittoRepo(config DittoConfig) DittoRepo {
	// Set default workers if not specified
	if config.Workers <= 0 {
		config.Workers = defaultWorkers
	}

	// Backwards compatibility: if Dists is empty but Dist is set, use Dist
	if len(config.Dists) == 0 && config.Dist != "" {
		config.Dists = []string{config.Dist}
	}

	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	if config.FileSystem == nil {
		config.FileSystem = NewOsFileSystem()
	}

	if config.Downloader == nil {
		config.Downloader = NewHTTPDownloader(config.FileSystem)
	}

	return &dittoRepo{
		config:        config,
		logger:        config.Logger,
		fs:            config.FileSystem,
		downloader:    config.Downloader,
		validPackages: make(map[string]bool),
	}
}

// packageMeta holds the download path and integrity data for a single .deb
type packageMeta struct {
	Path   string
	SHA256 string
}

// downloadJob represents a task for the worker pool
type downloadJob struct {
	URL      string
	Dest     string
	Checksum string
}

// verificationJob represents a file to be checked against a checksum
type verificationJob struct {
	pkg       packageMeta
	localPath string
}

func (d *dittoRepo) Mirror(ctx context.Context) <-chan ProgressUpdate {
	// Create progress channel
	d.progressChan = make(chan ProgressUpdate, 100)
	d.packagesDownloaded = 0
	d.totalPackages = 0

	// Start mirroring in a goroutine
	go func() {
		defer close(d.progressChan)
		d.doMirror(ctx)
	}()

	return d.progressChan
}

func (d *dittoRepo) doMirror(ctx context.Context) {
	// Iterate over all distributions
	for _, dist := range d.config.Dists {
		if ctx.Err() != nil {
			d.logger.Error(fmt.Sprintf("Context cancelled: %v", ctx.Err()))
			return
		}

		d.logger.Info(fmt.Sprintf("Starting mirror of %s [%s]...\n", d.config.RepoURL, dist))

		if err := d.mirrorDistribution(ctx, dist); err != nil {
			d.logger.Error(fmt.Sprintf("Failed to mirror distribution %s: %v", dist, err))
			// Continue with other distributions
		}
	}

	// Clean up packages that no longer exist upstream
	if ctx.Err() == nil {
		if err := d.cleanupOrphanedPackages(); err != nil {
			d.logger.Warn(fmt.Sprintf("Error during cleanup: %v\n", err))
		}
	}

	d.logger.Info("Mirror complete.")
}

func (d *dittoRepo) mirrorDistribution(ctx context.Context, dist string) error {
	// Check context before starting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 1. Fetch Repository Metadata (Signatures & Release file)
	// We must fetch these byte-for-byte to preserve upstream signatures.
	metadataFiles := []string{"InRelease", "Release", "Release.gpg"}
	for _, meta := range metadataFiles {
		// Check context
		if ctx.Err() != nil {
			return ctx.Err()
		}
		url := fmt.Sprintf("%s/dists/%s/%s", d.config.RepoURL, dist, meta)
		dest := path.Join(d.config.DownloadPath, "dists", dist, meta)

		d.logger.Info(fmt.Sprintf("Fetching Metadata: %s... ", meta))
		// We pass "" as checksum because we don't know it yet (it's the source of truth)
		if _, err := d.downloader.DownloadFile(url, dest, ""); err != nil {
			// InRelease is optional if Release.gpg exists, but usually good to have.
			// Release and Release.gpg are critical.
			d.logger.Warn(fmt.Sprintf("%v\n", err))
		} else {
			d.logger.Info("OK")
		}
	}

	// 2. Read the local 'Release' file to parse package indices
	// We read from disk instead of fetching again to ensure consistency.
	releasePath := path.Join(d.config.DownloadPath, "dists", dist, "Release")
	releaseBytes, err := d.fs.ReadFile(releasePath)
	if err != nil {
		return fmt.Errorf("could not read local Release file: %w", err)
	}

	indices := d.parseReleaseFile(string(releaseBytes))

	// 3. Process each Package Index (Packages, Translations, possibly cnfs)
	for _, idxPath := range indices {
		// Check context
		if ctx.Err() != nil {
			return ctx.Err()
		}

		d.logger.Info(fmt.Sprintf("Processing Index: %s\n", idxPath))

		fullIndexURL := fmt.Sprintf("%s/dists/%s/%s", d.config.RepoURL, dist, idxPath)
		localIndexPath := path.Join(d.config.DownloadPath, "dists", dist, idxPath)

		// Download the Index (Packages.gz) itself
		// Note: Ideally, we should verify the SHA256 of this index file against the Release file here.
		// For this prototype, we just download it.
		calculatedHash, err := d.downloader.DownloadFile(fullIndexURL, localIndexPath, "")
		if err != nil {
			d.logger.Warn(fmt.Sprintf("  Failed to download index: %v\n", err))
			continue
		}

		// We have the file and its hash. Create the alias so modern clients are happy.
		if err := d.createByHashLink(localIndexPath, calculatedHash); err != nil {
			d.logger.Warn(fmt.Sprintf("  Failed to create by-hash link: %v\n", err))
		}

		// Only looks for .debs inside "Packages" files, not "Translation" files
		if strings.Contains(idxPath, "Packages") {
			d.processPackageIndex(ctx, localIndexPath)
		}
	}

	return nil
}

// processPackageIndex parses the index and spins up workers to download missing files
func (d *dittoRepo) processPackageIndex(ctx context.Context, localIndexPath string) {
	debs, err := d.extractDebsFromIndex(localIndexPath)
	if err != nil {
		d.logger.Error(fmt.Sprintf("  Error parsing index: %v\n", err))
		return
	}

	d.logger.Info(fmt.Sprintf("  -> Found %d packages. Checking pool...\n", len(debs)))

	// Track all valid packages from this index
	d.mu.Lock()
	for _, pkg := range debs {
		d.validPackages[pkg.Path] = true
	}
	d.totalPackages += len(debs)
	d.mu.Unlock()

	// 1. Set up verification worker pool
	verificationJobs := make(chan verificationJob, len(debs))
	downloadJobsChan := make(chan downloadJob, len(debs))
	var verificationWg sync.WaitGroup

	for w := 0; w < d.config.Workers; w++ {
		verificationWg.Add(1)
		go func(workerID int) {
			defer verificationWg.Done()
			for job := range verificationJobs {
				// Check context before processing
				if ctx.Err() != nil {
					return
				}

				// Check if file already exists
				if _, err := d.fs.Stat(job.localPath); err == nil {
					// File exists, verify checksum
					d.logger.Debug(fmt.Sprintf("[Verifier %d] Verifying existing: %s... ", workerID, job.pkg.Path))
					match, err := d.verifyFile(job.localPath, job.pkg.SHA256)
					if err != nil {
						d.logger.Warn(fmt.Sprintf("[Verifier %d] Error verifying %s: %v", workerID, job.pkg.Path, err))
					} else if match {
						d.logger.Debug(fmt.Sprintf("[Verifier %d] OK (Skipping download): %s", workerID, job.pkg.Path))
						continue // Checksum matches, skip to next job
					} else {
						d.logger.Info(fmt.Sprintf("[Verifier %d] Mismatch (Redownloading): %s", workerID, job.pkg.Path))
					}
				}

				// If file doesn't exist or checksum mismatches, queue for download
				select {
				case downloadJobsChan <- downloadJob{
					URL:      fmt.Sprintf("%s/%s", d.config.RepoURL, job.pkg.Path),
					Dest:     job.localPath,
					Checksum: job.pkg.SHA256,
				}:
				case <-ctx.Done():
					return
				}
			}
		}(w)
	}

	// 2. Send verification jobs
	for _, pkg := range debs {
		localPath := path.Join(d.config.DownloadPath, pkg.Path)
		select {
		case verificationJobs <- verificationJob{
			pkg:       pkg,
			localPath: localPath,
		}:
		case <-ctx.Done():
			return // Exit the goroutine
		}
	}
	close(verificationJobs)

	// 3. Wait for verification to finish and collect download jobs
	go func() {
		verificationWg.Wait()
		close(downloadJobsChan)
	}()

	var jobs []downloadJob
	for job := range downloadJobsChan {
		jobs = append(jobs, job)
	}

	if len(jobs) == 0 {
		d.logger.Info("  -> All packages already up to date.")
		return
	}

	d.logger.Info(fmt.Sprintf("  -> Queuing %d downloads across %d workers...\n", len(jobs), d.config.Workers))

	// 4. Set up worker pool for downloads
	jobChan := make(chan downloadJob, len(jobs))
	var wg sync.WaitGroup

	// Spin up workers
	for w := 0; w < d.config.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobChan {
				// Check context before processing
				if ctx.Err() != nil {
					return
				}

				filename := path.Base(job.Dest)
				_, err := d.downloader.DownloadFile(job.URL, job.Dest, job.Checksum)
				if err != nil {
					d.logger.Warn(fmt.Sprintf("[Worker %d] FAILED %s: %v", workerID, filename, err))
				} else {
					// Minimal output to keep console clean - debug log only
					d.logger.Debug(fmt.Sprintf("[Worker %d] Downloaded %s", workerID, filename))

					// Send progress update
					d.mu.Lock()
					d.packagesDownloaded++
					select {
					case d.progressChan <- ProgressUpdate{
						PackagesDownloaded: d.packagesDownloaded,
						TotalPackages:      d.totalPackages,
						CurrentFile:        filename,
					}:
					default:
						// Channel full, skip this update
					}
					d.mu.Unlock()
				}
			}
		}(w)
	}

	// 5. Send jobs
	for _, j := range jobs {
		select {
		case <-ctx.Done():
			close(jobChan)
			wg.Wait()
			return
		case jobChan <- j:
		}
	}
	close(jobChan)

	// 6. Wait for completion
	wg.Wait()
	d.logger.Info("  -> Downloads for this index finished.")
}

// parseReleaseFile extracts paths to Packages.gz that match our Arch/Component filter
// Also suports Translation files (bz2, usually)
func (d *dittoRepo) parseReleaseFile(content string) []string {
	var relevantFiles []string
	scanner := bufio.NewScanner(strings.NewReader(content))
	inSha256Block := false

	for scanner.Scan() {
		line := scanner.Text()

		// The Release file format has "SHA256:" followed by indented lines of files
		if strings.HasPrefix(line, "SHA256:") {
			inSha256Block = true
			continue
		}
		// If we hit another key (no indentation), we exited the block
		if inSha256Block && len(line) > 0 && line[0] != ' ' {
			inSha256Block = false
		}

		if inSha256Block {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				continue
			}
			filePath := parts[2] // Format: checksum size filename

			validExt := strings.HasSuffix(filePath, ".gz") ||
				strings.HasSuffix(filePath, ".xz") ||
				strings.HasSuffix(filePath, ".bz2")

			// Filter: We only want "Packages.gz" or "Packages.xz"
			if !validExt {
				continue
			}

			// Filter: Check if this file belongs to our desired Components/Archs
			// Path looks like: main/binary-amd64/Packages.gz
			if d.isDesired(filePath) {
				relevantFiles = append(relevantFiles, filePath)
			}
		}
	}
	return relevantFiles
}

// isDesired checks if a file path string matches our Component/Arch config
func (d *dittoRepo) isDesired(filePath string) bool {
	// Check Component
	matchedComponent := false
	for _, c := range d.config.Components {
		if strings.HasPrefix(filePath, c+"/") {
			matchedComponent = true
			break
		}
	}
	if !matchedComponent {
		return false
	}

	// Check Type: Architecture Binary OR Translation
	isBinary := false
	for _, a := range d.config.Archs {
		if strings.Contains(filePath, "binary-"+a+"/") && strings.Contains(filePath, "Packages") {
			isBinary = true
			break
		}
	}

	isTranslation := false
	if strings.Contains(filePath, "i18n/Translation-") {
		for _, lang := range d.config.Languages {
			// Matches Translation-en.gz, Translation-en_GB.bz2, etc.
			if strings.Contains(filePath, "Translation-"+lang) {
				isTranslation = true
				break
			}
		}
	}

	// Check CNF (command-not-found) files
	isCnf := false
	if strings.Contains(filePath, "cnf/Commands-") {
		for _, a := range d.config.Archs {
			// Matches Commands-amd64.xz, Commands-arm64.xz, etc.
			if strings.Contains(filePath, "Commands-"+a) {
				isCnf = true
				break
			}
		}
	}

	return isBinary || isTranslation || isCnf
}

// extractDebsFromIndex parses a local Packages.gz file
// returning a list of packageMeta objects with filenames and checksums.
func (d *dittoRepo) extractDebsFromIndex(localPath string) (packages []packageMeta, err error) {
	f, err := d.fs.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// Handle GZIP automatically
	var reader io.Reader = f
	if strings.HasSuffix(localPath, ".gz") {
		gzReader, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer func() {
			if cerr := gzReader.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}()
		reader = gzReader
	} else if strings.HasSuffix(localPath, ".xz") {
		// Note: Standard Go library doesn't support XZ.
		// We would need "github.com/ulikunitz/xz" or simply avoid .xz indices if possible.
		return nil, fmt.Errorf("xz compression not implemented")
	}

	scanner := bufio.NewScanner(reader)

	// Increase buffer size to handle ver long lines (Debian Description fields can be huge)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 5*1024*1024)

	// State variables for the current block
	var currentPkg packageMeta
	inBlock := false

	for scanner.Scan() {
		line := scanner.Text()

		// A blank line indicates the end of a package stanza
		if strings.TrimSpace(line) == "" {
			if inBlock && currentPkg.Path != "" && currentPkg.SHA256 != "" {
				packages = append(packages, currentPkg)
			}
			// Reset for next block
			currentPkg = packageMeta{}
			inBlock = false
			continue
		}

		inBlock = true

		// Simple prefix parsing.
		// Note: A robust parser usually handles multiline values (lines starting with space).
		// but Filename and SHA256 are always single lines in standard Debian repos.
		if strings.HasPrefix(line, "Filename: ") {
			currentPkg.Path = strings.TrimPrefix(line, "Filename: ")
		} else if strings.HasPrefix(line, "SHA256: ") {
			currentPkg.SHA256 = strings.TrimPrefix(line, "SHA256: ")
		}
	}

	// Handle the very last block if the file doesn't end with a newline
	if inBlock && currentPkg.Path != "" && currentPkg.SHA256 != "" {
		packages = append(packages, currentPkg)
	}

	return packages, scanner.Err()
}

// verifyFile is a helper method to check a downloaded file against the expected checksum
func (d *dittoRepo) verifyFile(filepath string, expectedSHA256 string) (match bool, err error) {
	f, err := d.fs.Open(filepath)
	if err != nil {
		return false, err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return false, err
	}

	calculated := hex.EncodeToString(hasher.Sum(nil))
	return calculated == expectedSHA256, nil
}

// createByHashLink creates a hardlink (or copy) in the by-hash/SHA256/ directory
func (d *dittoRepo) createByHashLink(originalPath string, hash string) (err error) {
	// originalPath: .../main/binary-amd64/Packages.gz
	// targetDir:    .../main/binary-amd64/by-hash/SHA256
	dir := filepath.Dir(originalPath)
	byHashDir := filepath.Join(dir, "by-hash", "SHA256")

	if err := d.fs.MkdirAll(byHashDir, 0o755); err != nil {
		return err
	}

	targetPath := filepath.Join(byHashDir, hash)

	// Remove existing if present to ensure freshness
	if err := d.fs.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Try Hardlink first (fastest, saves space)
	err = d.fs.Link(originalPath, targetPath)
	if err != nil {
		// Fallback to Copy if hardlink fails (e.g. cross-device)
		src, err := d.fs.Open(originalPath)
		if err != nil {
			return err
		}
		defer func() {
			if cerr := src.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}()
		dst, err := d.fs.Create(targetPath)
		if err != nil {
			return err
		}
		defer func() {
			if cerr := dst.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}()
		_, err = io.Copy(dst, src)
		return err
	}
	return nil
}

// cleanupOrphanedPackages removes .deb files from the pool that are no longer referenced upstream
func (d *dittoRepo) cleanupOrphanedPackages() error {
	poolPath := filepath.Join(d.config.DownloadPath, "pool")

	// Check if pool directory exists
	if _, err := d.fs.Stat(poolPath); err != nil {
		// Pool doesn't exist yet, nothing to clean
		return nil
	}

	d.logger.Info("Scanning for orphaned packages...")

	var toRemove []string
	err := d.fs.WalkDir(poolPath, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if de.IsDir() {
			return nil
		}

		// Only consider .deb files
		if !strings.HasSuffix(path, ".deb") {
			return nil
		}

		// Get relative path from download root
		relPath, err := filepath.Rel(d.config.DownloadPath, path)
		if err != nil {
			return err
		}

		// Convert to forward slashes for consistent comparison
		relPath = filepath.ToSlash(relPath)

		// Check if this package is in our valid set
		d.mu.Lock()
		isValid := d.validPackages[relPath]
		d.mu.Unlock()

		if !isValid {
			toRemove = append(toRemove, path)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("error walking pool directory: %w", err)
	}

	if len(toRemove) == 0 {
		d.logger.Info("No orphaned packages found.")
		return nil
	}

	d.logger.Info(fmt.Sprintf("Removing %d orphaned packages...", len(toRemove)))
	for _, path := range toRemove {
		relPath, _ := filepath.Rel(d.config.DownloadPath, path)
		d.logger.Debug(fmt.Sprintf("Removing: %s", relPath))
		if err := d.fs.Remove(path); err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to remove %s: %v", relPath, err))
		}
	}

	d.logger.Info("Cleanup complete.")
	return nil
}
