package main

import (
	"log/slog"

	"github.com/canonical/ditto-repo/repo"
)

// -- Configuration --
var config = repo.DittoConfig{
	RepoURL:      "http://ppa.launchpadcontent.net/mitchburton/snap-http/ubuntu",
	Dist:         "noble",
	Components:   []string{"main"},
	Archs:        []string{"amd64"},
	Languages:    []string{"en"},
	DownloadPath: "./mirror",
}

func main() {
	logger := slog.Default()
	d := repo.NewDittoRepo(config, logger)
	err := d.Mirror()
	if err != nil {
		panic(err)
	}
}
