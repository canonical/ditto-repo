# ditto-repo

> [!WARNING]
> ditto-repo is currently in very early development, and is mainly a proof-of-concept. It may change rapidly.

**ditto-repo** is a lightweight, purely Golang-based tool for mirroring Debian repositories.

ditto-repo is designed to be a **signature-preserving smart scraper**. It downloads upstream repositories byte-for-byte, ensuring that the original GPG signatures (`Release.gpg`, `InRelease`) remain valid. This allows you to host a partial mirror (specific architectures or components) that clients can trust using the original upstream public keys.

## Features

* **Signature Preservation:** Does not modify metadata. Downloads `InRelease` and `Release.gpg` exactly as they exist upstream.
* **Partial Mirroring:** Filter by specific **Distributions** (e.g., `bookworm`), **Components** (e.g., `main`), **Architectures** (e.g., `amd64`), and **Languages**.
* **Atomic Downloads:** Downloads to temporary files and atomically renames them upon successful completion to prevent corrupt files in the mirror.
* **Data Integrity:** Verifies SHA256 checksums of all downloaded indices and packages against the upstream `Release` file.
* **Modern Apt Support:** Automatically creates `by-hash` directory structures (via hardlinks) required by modern `apt` clients.
* **Bandwidth Efficient:** Skips files that already exist locally by comparing SHA256 hashes.

## Prerequisites

* **Go 1.22+** installed on your machine.
* Disk space appropriate for the size of the repository you intend to mirror.

## Installation

1.  **Clone or copy the source:**

2.  **Build the binary:**
    ```bash
    go build -o ditto ./main.go
    ```

3. **Run it:**
   ```bash
   ./ditto
   ```

## Configuration

Currently, configuration is defined at the top of the `main.go` file. Open the file and modify the variables in the **Configuration** section:

```go
var (
    // The upstream repository URL
    RepoURL      = "http://archive.ubuntu.com/ubuntu"
    
    // The distribution codename (e.g., noble, jammy)
    Dist         = "noble"
    
    // Components to mirror
    Components   = []string{"main", "restricted"}
    
    // Architectures to download binary packages for
    Archs        = []string{"amd64"}
    
    // Languages for translation files (e.g., "en", "es")
    Languages    = []string{"en"}
    
    // Local directory where the mirror will be stored
    DownloadPath = "./mirror"
)
