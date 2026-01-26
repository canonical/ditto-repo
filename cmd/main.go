package main

import (
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
	d := repo.NewDittoRepo(config)
	err := d.Mirror()
	if err != nil {
		panic(err)
	}
}
