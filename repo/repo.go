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
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultWorkers = 5
)

// VerifyMode controls how already-existing pool files are checked before
// deciding whether to re-download them.
type VerifyMode string

const (
	// VerifyChecksum reads the file and computes its SHA256 hash (default, most accurate).
	VerifyChecksum VerifyMode = "checksum"
	// VerifySize only compares the on-disk file size against the index metadata.
	// Faster than a full hash but cannot detect silent corruption.
	VerifySize VerifyMode = "size"
)

// ProgressUpdate represents a progress event during mirroring
type ProgressUpdate struct {
	PackagesDownloaded int
	PackagesVerified   int
	TotalPackages      int
	CurrentFile        string
}

// The canonical implementation of DittoRepo
type dittoRepo struct {
	config             DittoConfig
	logger             Logger
	fs                 FileSystem
	downloader         Downloader
	mu                 sync.Mutex // Protect progress counters
	progressChan       chan ProgressUpdate
	packagesDownloaded int
	packagesVerified   int
	totalPackages      int
}

// DittoConfig holds all configuration for the mirroring process
type DittoConfig struct {
	RepoURL      string     `json:"repo-url"`
	Dist         string     `json:"dist"`  // Deprecated: use Dists instead
	Dists        []string   `json:"dists"` // List of distributions to mirror
	Components   []string   `json:"components"`
	Archs        []string   `json:"archs"`
	Languages    []string   `json:"languages"`     // Add languages here (e.g. "en", "es")
	DownloadPath string     `json:"download-path"` // Local storage root
	Workers             int        `json:"workers"`               // Number of concurrent download workers
	VerifyMode          VerifyMode `json:"verify-mode"`           // How existing pool files are checked (default: checksum)
	AllowMissingIndices bool       `json:"allow-missing-indices"` // Warn instead of failing when a Packages index file cannot be fetched

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

	// Default to checksum verification
	if config.VerifyMode == "" {
		config.VerifyMode = VerifyChecksum
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
		config:     config,
		logger:     config.Logger,
		fs:         config.FileSystem,
		downloader: config.Downloader,
	}
}

// packageMeta holds the download path and integrity data for a single .deb
type packageMeta struct {
	Path   string
	SHA256 string
	Size   int64
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
	d.packagesVerified = 0
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
	var mirrorErr bool
	for _, dist := range d.config.Dists {
		if ctx.Err() != nil {
			d.logger.Error(fmt.Sprintf("Context cancelled: %v", ctx.Err()))
			return
		}

		d.logger.Info(fmt.Sprintf("Starting mirror of %s [%s]...\n", d.config.RepoURL, dist))

		if err := d.mirrorDistribution(ctx, dist); err != nil {
			d.logger.Error(fmt.Sprintf("cannot mirror distribution %s: %v", dist, err))
			mirrorErr = true
			// Continue with other distributions
		}
	}

	// Clean up packages that no longer exist upstream.
	// Skip if any distribution failed: on-disk indices would be incomplete and
	// packages from the failed distribution would be incorrectly removed.
	if ctx.Err() == nil && !mirrorErr {
		if err := d.cleanupOrphanedPackages(); err != nil {
			d.logger.Warn(fmt.Sprintf("cannot clean up: %v\n", err))
		}
	} else if mirrorErr {
		d.logger.Warn("Skipping cleanup: one or more distributions failed to sync")
	}

	// Post-sync consistency check: re-fetch each distribution's Release file and
	// compare it to what we downloaded at the start. If the upstream changed during
	// the sync we re-run only the affected distributions before declaring success.
	if ctx.Err() == nil {
		var staleDists []string
		d.logger.Info("Performing post-update consistency check")

		for _, dist := range d.config.Dists {
			if ctx.Err() != nil {
				break
			}
			fresh, err := d.isDistributionFresh(ctx, dist)
			if err != nil {
				d.logger.Warn(fmt.Sprintf("cannot check freshness of %s: %v", dist, err))
				staleDists = append(staleDists, dist)
			} else if !fresh {
				d.logger.Warn(fmt.Sprintf("Distribution %s changed during sync, will re-sync", dist))
				staleDists = append(staleDists, dist)
			}
		}
		if len(staleDists) > 0 {
			d.logger.Warn(fmt.Sprintf("Re-syncing %d stale distribution(s)...", len(staleDists)))
			for _, dist := range staleDists {
				if ctx.Err() != nil {
					break
				}
				if err := d.mirrorDistribution(ctx, dist); err != nil {
					d.logger.Error(fmt.Sprintf("cannot re-sync distribution %s: %v", dist, err))
				}
			}
		}
	}

	if ctx.Err() != nil {
		return
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
		return fmt.Errorf("cannot read local Release file: %v", err)
	}

	indices := d.parseReleaseFile(string(releaseBytes))

	// 3. Download all index files first (Packages, Translations, cnf, etc.)
	// Track which local paths were successfully downloaded for the next phase.
	downloadedIndices := make([]string, 0, len(indices))
	for _, idxPath := range indices {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		d.logger.Info(fmt.Sprintf("Fetching Index: %s\n", idxPath))

		fullIndexURL := fmt.Sprintf("%s/dists/%s/%s", d.config.RepoURL, dist, idxPath)
		localIndexPath := path.Join(d.config.DownloadPath, "dists", dist, idxPath)

		calculatedHash, err := d.downloader.DownloadFile(fullIndexURL, localIndexPath, "")
		if err != nil {
			if d.config.AllowMissingIndices {
				d.logger.Warn(fmt.Sprintf("cannot download index %s: %v (skipping)", idxPath, err))
				continue
			}
			return fmt.Errorf("cannot download index %s: %w", idxPath, err)
		}

		// We have the file and its hash. Create the alias so modern clients are happy.
		if err := d.createByHashLink(localIndexPath, calculatedHash); err != nil {
			d.logger.Warn(fmt.Sprintf("  cannot create by-hash link: %v\n", err))
		}

		downloadedIndices = append(downloadedIndices, localIndexPath)
	}

	// 4. Parse all Packages indices to build a complete, unified package list.
	var allDebs []packageMeta
	seen := make(map[string]bool)
	parsedStems := make(map[string]bool) // tracks base paths already parsed (without compression ext)
	for _, localIndexPath := range downloadedIndices {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if !strings.Contains(localIndexPath, "Packages") {
			continue
		}

		// Derive the stem by stripping the compression extension.
		stem := localIndexPath
		for _, ext := range []string{".gz", ".xz", ".bz2"} {
			if strings.HasSuffix(stem, ext) {
				stem = strings.TrimSuffix(stem, ext)
				break
			}
		}
		if parsedStems[stem] {
			d.logger.Info(fmt.Sprintf("Skipping Index (stem already parsed): %s\n", localIndexPath))
			continue
		}

		d.logger.Info(fmt.Sprintf("Parsing Index: %s\n", localIndexPath))
		debs, err := d.extractDebsFromIndex(localIndexPath)
		if err != nil {
			d.logger.Warn(fmt.Sprintf("  cannot parse index %s: %v\n", localIndexPath, err))
			continue
		}
		parsedStems[stem] = true
		d.logger.Info(fmt.Sprintf("  -> Found %d packages.\n", len(debs)))

		for _, pkg := range debs {
			if !seen[pkg.Path] {
				seen[pkg.Path] = true
				allDebs = append(allDebs, pkg)
			}
		}
	}

	// 5. Now download the complete package set in one pass.
	if len(allDebs) > 0 {
		d.downloadPackages(ctx, allDebs)
	}

	return nil
}

// downloadPackages verifies and downloads a pre-built list of packages using a worker pool.
func (d *dittoRepo) downloadPackages(ctx context.Context, debs []packageMeta) {
	d.logger.Info(fmt.Sprintf("Checking pool for %d unique packages...\n", len(debs)))

	d.mu.Lock()
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
				if info, err := d.fs.Stat(job.localPath); err == nil {
					// File exists, check it according to the configured verify mode.
					d.logger.Debug(fmt.Sprintf("[Verifier %d] Verifying existing: %s... ", workerID, job.pkg.Path))
					var ok bool
					switch d.config.VerifyMode {
					case VerifySize:
						ok = info.Size() == job.pkg.Size
					default: // VerifyChecksum
						var err error
						ok, err = d.verifyFile(job.localPath, job.pkg.SHA256)
						if err != nil {
							d.logger.Warn(fmt.Sprintf("[Verifier %d] cannot verify %s: %v", workerID, job.pkg.Path, err))
						}
					}
					if ok {
						d.logger.Debug(fmt.Sprintf("[Verifier %d] OK (Skipping download): %s", workerID, job.pkg.Path))
						d.sendVerificationProgress(path.Base(job.localPath))
						continue
					}
					d.logger.Info(fmt.Sprintf("[Verifier %d] Mismatch (Redownloading): %s", workerID, job.pkg.Path))
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
				d.sendVerificationProgress(path.Base(job.localPath))
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
					d.logger.Warn(fmt.Sprintf("[Worker %d] cannot download %s: %v", workerID, filename, err))
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
	d.logger.Info("  -> Package downloads finished.")
}

// sendVerificationProgress increments the verified counter and emits a ProgressUpdate.
func (d *dittoRepo) sendVerificationProgress(filename string) {
	d.mu.Lock()
	d.packagesVerified++
	select {
	case d.progressChan <- ProgressUpdate{
		PackagesDownloaded: d.packagesDownloaded,
		PackagesVerified:   d.packagesVerified,
		TotalPackages:      d.totalPackages,
		CurrentFile:        filename,
	}:
	default:
		// Channel full, skip this update
	}
	d.mu.Unlock()
}

// isDistributionFresh re-fetches the upstream Release file for dist and compares its
// hash to the local copy. Returns false when the upstream has been updated since the
// sync began, meaning another pass is required.
func (d *dittoRepo) isDistributionFresh(ctx context.Context, dist string) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	localReleasePath := path.Join(d.config.DownloadPath, "dists", dist, "Release")
	upstreamURL := fmt.Sprintf("%s/dists/%s/Release", d.config.RepoURL, dist)
	tmpPath := localReleasePath + ".check"
	defer func() { _ = d.fs.Remove(tmpPath) }()

	// Download the current upstream Release to a temp file and capture its hash.
	upstreamHash, err := d.downloader.DownloadFile(upstreamURL, tmpPath, "")
	if err != nil {
		return false, fmt.Errorf("cannot fetch upstream Release: %w", err)
	}

	// Check whether our local copy matches.
	match, err := d.verifyFile(localReleasePath, upstreamHash)
	if err != nil {
		return false, fmt.Errorf("cannot verify local Release: %w", err)
	}

	return match, nil
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
func (d *dittoRepo) extractDebsFromIndex(localPath string) ([]packageMeta, error) {
	f, err := d.fs.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Handle GZIP automatically
	var reader io.Reader = f
	if strings.HasSuffix(localPath, ".gz") {
		gzReader, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer gzReader.Close()
		reader = gzReader
	} else if strings.HasSuffix(localPath, ".xz") {
		// Note: Standard Go library doesn't support XZ.
		// We would need "github.com/ulikunitz/xz" or simply avoid .xz indices if possible.
		return nil, fmt.Errorf("xz compression not implemented")
	}

	var packages []packageMeta
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
		} else if strings.HasPrefix(line, "Size: ") {
			if n, err := strconv.ParseInt(strings.TrimPrefix(line, "Size: "), 10, 64); err == nil {
				currentPkg.Size = n
			}
		}
	}

	// Handle the very last block if the file doesn't end with a newline
	if inBlock && currentPkg.Path != "" && currentPkg.SHA256 != "" {
		packages = append(packages, currentPkg)
	}

	return packages, scanner.Err()
}

// verifyFile is a helper method to check a downloaded file against the expected checksum
func (d *dittoRepo) verifyFile(filepath string, expectedSHA256 string) (bool, error) {
	f, err := d.fs.Open(filepath)
	if err != nil {
		return false, err
	}
	defer f.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return false, err
	}

	calculated := hex.EncodeToString(hasher.Sum(nil))
	return calculated == expectedSHA256, nil
}

// createByHashLink creates a hardlink (or copy) in the by-hash/SHA256/ directory
func (d *dittoRepo) createByHashLink(originalPath string, hash string) error {
	// originalPath: .../main/binary-amd64/Packages.gz
	// targetDir:    .../main/binary-amd64/by-hash/SHA256
	dir := filepath.Dir(originalPath)
	byHashDir := filepath.Join(dir, "by-hash", "SHA256")

	if err := d.fs.MkdirAll(byHashDir, 0o755); err != nil {
		return err
	}

	targetPath := filepath.Join(byHashDir, hash)

	// Remove existing if present to ensure freshness
	_ = d.fs.Remove(targetPath)

	// Try Hardlink first (fastest, saves space)
	err := d.fs.Link(originalPath, targetPath)
	if err != nil {
		// Fallback to Copy if hardlink fails (e.g. cross-device)
		src, err := d.fs.Open(originalPath)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := d.fs.Create(targetPath)
		if err != nil {
			return err
		}
		defer dst.Close()
		_, err = io.Copy(dst, src)
		return err
	}
	return nil
}

// cleanupOrphanedPackages removes .deb files from the pool that are no longer referenced
// by any on-disk Packages index. It scans all indices under the dists/ tree so that
// packages belonging to distributions not in the current config are preserved.
func (d *dittoRepo) cleanupOrphanedPackages() error {
	poolPath := filepath.Join(d.config.DownloadPath, "pool")

	// Check if pool directory exists
	if _, err := d.fs.Stat(poolPath); err != nil {
		// Pool doesn't exist yet, nothing to clean
		return nil
	}

	// Build valid set from every Packages index present on disk.
	distsPath := filepath.Join(d.config.DownloadPath, "dists")
	validOnDisk := make(map[string]bool)
	parsedStems := make(map[string]bool)

	if _, err := d.fs.Stat(distsPath); err == nil {
		walkErr := d.fs.WalkDir(distsPath, func(p string, de fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if de.IsDir() {
				return nil
			}
			base := filepath.Base(p)
			if !strings.HasPrefix(base, "Packages") {
				return nil
			}
			if !strings.HasSuffix(p, ".gz") && !strings.HasSuffix(p, ".xz") && !strings.HasSuffix(p, ".bz2") {
				return nil
			}
			// Deduplicate by stem so we don't parse the same index twice
			stem := p
			for _, ext := range []string{".gz", ".xz", ".bz2"} {
				if strings.HasSuffix(stem, ext) {
					stem = strings.TrimSuffix(stem, ext)
					break
				}
			}
			if parsedStems[stem] {
				return nil
			}
			parsedStems[stem] = true

			debs, err := d.extractDebsFromIndex(p)
			if err != nil {
				d.logger.Warn(fmt.Sprintf("cleanup: cannot parse index %s: %v", p, err))
				return nil
			}
			for _, pkg := range debs {
				validOnDisk[pkg.Path] = true
			}
			return nil
		})
		if walkErr != nil {
			return fmt.Errorf("cannot scan dists directory: %v", walkErr)
		}
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

		if !validOnDisk[relPath] {
			toRemove = append(toRemove, path)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("cannot walk pool directory: %v", err)
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
			d.logger.Warn(fmt.Sprintf("cannot remove %s: %v", relPath, err))
		}
	}

	d.logger.Info("Cleanup complete.")
	return nil
}
