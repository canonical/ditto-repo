# ditto-repo

> [!WARNING]
> ditto-repo is currently in very early development, and is mainly a proof-of-concept. It may change rapidly.

**ditto-repo** is a lightweight, purely Golang-based tool for mirroring Debian repositories.

ditto-repo is designed to be a **signature-preserving smart scraper**. It downloads upstream repositories byte-for-byte, ensuring that the original GPG signatures (`Release.gpg`, `InRelease`) remain valid. This allows you to host a partial mirror (specific architectures or components) that clients can trust using the original upstream public keys.

## Features

* **Signature Preservation:** Does not modify metadata. Downloads `InRelease` and `Release.gpg` exactly as they exist upstream.
* **Partial Mirroring:** Filter by specific **Distributions** (e.g., `noble`), **Components** (e.g., `main`), **Architectures** (e.g., `amd64`), and **Languages**.
* **Multi-Mirror Aggregation:** Combine multiple upstream hosts that publish an identical `Release` file (e.g., `archive.ubuntu.com` and `ports.ubuntu.com`) into a single mirror, with `Release` consistency validation and per-file failover.
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

First it checks for a configuration file path specified via the `--config-path` CLI flag or `DITTO_CONFIG_PATH` environment variable. If provided, it will attempt to read the configuration from that file.

If neither the CLI flag nor the environment variable are provided, it will look for a `ditto-config.json` file in the current directory.

As last resort it will use the embedded default configuration from `cmd/config.default.json`, which is suitable for testing and targets a repository that only contains a single all-architecture package.

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

* **repo-urls**: List of upstream mirror URLs to pull from (e.g., `["https://archive.ubuntu.com/ubuntu", "https://ports.ubuntu.com"]`). All listed mirrors must serve byte-identical `Release` files for every distribution; ditto verifies this before downloading and aborts if they differ. Files are fetched from the mirrors using failover (the first mirror that has a file wins), which lets you combine repositories that split content across hosts (for example, different architectures on `archive.ubuntu.com` vs `ports.ubuntu.com`).
* **repo-url**: A single upstream repository URL (deprecated, use `repo-urls` instead)
* **arch-urls**: Optional map of architecture to the mirror URL that should be tried *first* for that architecture's files (e.g., `{"arm64": "https://ports.ubuntu.com"}`). This is purely a performance hint to avoid wasted requests on split repositories; downloads still fall back to the full `repo-urls` list. For architectures you do *not* map explicitly, ditto learns the right mirror automatically: the first mirror that successfully serves an arch-specific file is cached and tried first for the remaining files of that architecture.
* **dists**: List of distribution codenames to mirror (e.g., ["noble", "jammy"])
* **dist**: Single distribution codename (deprecated, use dists instead)
* **components**: Components to mirror
* **archs**: Architectures to download binary packages for (also downloads `cnf/Commands-{arch}` files if they exist)
* **languages**: Languages for translation files (e.g., "en", "es")
* **download-path**: Local directory where the mirror will be stored
* **workers**: Number of concurrent downloads or checksum verifiers (default: 5)
* **verify-mode**: File verification mode for existing pool files: `checksum` (default) or `size`
* **allow-missing-indices**: When `true`, warn instead of failing when a Packages index file cannot be fetched (e.g. 404). Useful for repos where not every component/arch path is guaranteed to exist.

**Note:** The `dists` parameter is recommended for new configurations. The `dist` parameter is maintained for backwards compatibility. If both are specified, `dists` takes precedence. If only `dist` is specified, it will be converted to a single-element `dists` list.

**Note:** Likewise, `repo-urls` is recommended over the deprecated `repo-url`. If both are specified, `repo-urls` takes precedence. If only `repo-url` is specified, it is converted to a single-element `repo-urls` list.

### Environment Variables

All configuration options can be overridden using environment variables:

* **DITTO_CONFIG_PATH**
* **DITTO_REPO_URLS** (comma-separated list of mirror URLs)
* **DITTO_REPO_URL** (deprecated, use DITTO_REPO_URLS)
* **DITTO_ARCH_URLS** (comma-separated `arch=url` pairs, e.g. `arm64=https://ports.ubuntu.com`)
* **DITTO_DISTS** (comma-separated list)
* **DITTO_DIST** (deprecated, use DITTO_DISTS)
* **DITTO_COMPONENTS**
* **DITTO_ARCHS**
* **DITTO_LANGUAGES**
* **DITTO_DOWNLOAD_PATH**
* **DITTO_WORKERS**
* **DITTO_VERIFY_MODE** (`checksum` or `size`)
* **DITTO_ALLOW_MISSING_INDICES** (set to "true", "yes" or "1" to enable)
* **DITTO_DEBUG** (set to "true", "yes" or "1" to enable debug logging)

Example:
```bash
export DITTO_REPO_URL="http://archive.ubuntu.com/ubuntu"
export DITTO_DISTS="noble,jammy"
export DITTO_COMPONENTS="main,restricted"
./ditto
```

### CLI Flags

All configuration options can also be set via command-line flags, which take precedence over both environment variables and the configuration file:

* **--config-path**
* **--debug** (enable debug logging)
* **--repo-urls** (comma-separated list of mirror URLs)
* **--repo-url** (deprecated, use --repo-urls)
* **--arch-urls** (comma-separated `arch=url` pairs)
* **--dists** (comma-separated list)
* **--dist** (deprecated, use --dists)
* **--components**
* **--archs**
* **--languages**
* **--download-path**
* **--workers**
* **--verify-mode** (`checksum` or `size`)
* **--allow-missing-indices** (warn instead of failing on missing index files)

Example:
```bash
./ditto --repo-url="http://archive.ubuntu.com/ubuntu" --dists="noble,jammy" --components="main,restricted" --archs="amd64"
```

### Mirroring from Multiple Repositories

Some distributions split their content across multiple hosts. For example, Ubuntu serves
`amd64`/`i386` from `archive.ubuntu.com` and `arm64`/`armhf`/`riscv64`/etc. from
`ports.ubuntu.com`, even though both hosts publish an identical `Release` file for the same
suite. ditto can treat these as a single logical mirror:

```json
{
    "repo-urls": [
        "https://archive.ubuntu.com/ubuntu",
        "https://ports.ubuntu.com/ubuntu"
    ],
    "arch-urls": {
        "arm64": "https://ports.ubuntu.com/ubuntu",
        "armhf": "https://ports.ubuntu.com/ubuntu"
    },
    "dists": ["noble"],
    "components": ["main", "universe"],
    "archs": ["amd64", "arm64"],
    "download-path": "./mirror"
}
```

Before downloading anything, ditto fetches each distribution's `Release` file from every
configured mirror and verifies they are byte-identical. If any mirror disagrees, the run is
aborted (the package indices, and therefore checksums, would not be interchangeable). Each
file is then fetched using failover: the `arch-urls` hint is tried first when it applies,
otherwise mirrors are tried in `repo-urls` order until one succeeds. For architectures
without an explicit `arch-urls` entry, ditto remembers which mirror served the first
arch-specific file and tries that mirror first for the rest of that architecture, so the
extra fallback requests are only paid once.
