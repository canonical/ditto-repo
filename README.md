# ditto-repo

> [!WARNING]
> ditto-repo is currently in very early development, and is mainly a proof-of-concept. It may change rapidly.

**ditto-repo** is a lightweight, purely Golang-based tool for mirroring Debian repositories.

ditto-repo is designed to be a **signature-preserving smart scraper**. It downloads upstream repositories byte-for-byte, ensuring that the original GPG signatures (`Release.gpg`, `InRelease`) remain valid. This allows you to host a partial mirror (specific architectures or components) that clients can trust using the original upstream public keys.

## Features

* **Signature Preservation:** Does not modify metadata. Downloads `InRelease` and `Release.gpg` exactly as they exist upstream.
* **Partial Mirroring:** Filter by specific **Distributions** (e.g., `noble`), **Components** (e.g., `main`), **Architectures** (e.g., `amd64`), and **Languages**.
* **Command-Not-Found Support:** Automatically mirrors `cnf` directories (command-not-found data) if they exist in the upstream repository.
* **Atomic Downloads:** Downloads to temporary files and atomically renames them upon successful completion to prevent corrupt files in the mirror.
* **Data Integrity:** Verifies SHA256 checksums of all downloaded indices and packages against the upstream `Release` file.
* **Modern Apt Support:** Automatically creates `by-hash` directory structures (via hardlinks) required by modern `apt` clients.
* **Bandwidth Efficient:** Skips files that already exist locally by comparing SHA256 hashes.

## Usage Modes

ditto-repo can be used in two ways:

1. **As a Standalone Tool:** Run the compiled binary directly from the command line to mirror repositories based on your configuration.

2. **As a Library:** Import the `repo` package into your own Go applications to programmatically control repository mirroring with custom logic, configuration, or integration into larger systems.

The library provides clean interfaces for dependency injection, making it easy to customize filesystem operations (`FileSystem` interface), HTTP downloading (`Downloader` interface), and logging (`Logger` interface) to suit your needs.

For detailed information on using ditto-repo as a library, see [repo/README.md](repo/README.md).

## Prerequisites

* **Go 1.22+** installed on your machine.
* Disk space appropriate for the size of the repository you intend to mirror.

## Installation

1.  **Clone or copy the source:**

2.  **Build the binary:**
    ```bash
    go build -o ditto ./cmd/main.go
    ```

3. **Run it:**
   ```bash
   ./ditto
   ```

## Configuration

ditto-repo supports three configuration methods with the following priority order (highest to lowest):

1. **CLI flags** (highest priority)
2. **Environment variables**
3. **Configuration file** (`ditto-config.json`)

If a configuration file is not present, ditto-repo will use the embedded default configuration from `cmd/config.default.json`, which is suitable for testing and targets a repository that only contains a single all-architecture package.

> [!WARNING]
> Currently, configuration parsing and validation is not particularly sophisticated, so be sure to avoid typos and formatting errors.

### Configuration File

Create a `ditto-config.json` file in the directory where you run ditto-repo to customize your mirror settings.

Example `ditto-config.json`:

```json
{
    "repo-url": "http://archive.ubuntu.com/ubuntu",
    "dists": ["noble", "jammy"],
    "components": ["main", "restricted"],
    "archs": ["amd64"],
    "languages": ["en"],
    "download-path": "./mirror",
    "workers": 5
}
```

### Configuration Options

* **repo-url**: The upstream repository URL
* **dists**: List of distribution codenames to mirror (e.g., ["noble", "jammy"])
* **dist**: Single distribution codename (deprecated, use dists instead)
* **components**: Components to mirror
* **archs**: Architectures to download binary packages for (also downloads `cnf/Commands-{arch}` files if they exist)
* **languages**: Languages for translation files (e.g., "en", "es")
* **download-path**: Local directory where the mirror will be stored
* **workers**: Number of concurrent downloads or checksum verifiers (default: 5)

**Note:** The `dists` parameter is recommended for new configurations. The `dist` parameter is maintained for backwards compatibility. If both are specified, `dists` takes precedence. If only `dist` is specified, it will be converted to a single-element `dists` list.

### Environment Variables

All configuration options can be overridden using environment variables:

* **DITTO_REPO_URL**
* **DITTO_DISTS** (comma-separated list)
* **DITTO_DIST** (deprecated, use DITTO_DISTS)
* **DITTO_COMPONENTS**
* **DITTO_ARCHS**
* **DITTO_LANGUAGES**
* **DITTO_DOWNLOAD_PATH**
* **DITTO_WORKERS**
* **DITTO_DEBUG** (set to "true","yes" or 1 to enable debug logging)

Example:
```bash
export DITTO_REPO_URL="http://archive.ubuntu.com/ubuntu"
export DITTO_DISTS="noble,jammy"
export DITTO_COMPONENTS="main,restricted"
./ditto
```

### CLI Flags

All configuration options can also be set via command-line flags, which take precedence over both environment variables and the configuration file:

* **--debug** (enable debug logging)
* **--repo-url**
* **--dists** (comma-separated list)
* **--dist** (deprecated, use --dists)
* **--components**
* **--archs**
* **--languages**
* **--download-path**
* **--workers**

Example:
```bash
./ditto --repo-url="http://archive.ubuntu.com/ubuntu" --dists="noble,jammy" --components="main,restricted" --archs="amd64"
```
