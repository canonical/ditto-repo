package main

import (
	_ "embed"
	"encoding/json"
	"log"
	"os"

	"github.com/canonical/ditto-repo/repo"
)

//go:embed config.default.json
var defaultConfig []byte

func main() {
	var configData []byte

	// Try to read ditto-config.json from current directory
	data, err := os.ReadFile("ditto-config.json")
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

	d := repo.NewDittoRepo(config)
	err = d.Mirror()
	if err != nil {
		panic(err)
	}
}
