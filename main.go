package main

import (
	"embed"

	"github.com/monobilisim/monodb-manager/api"
)

// version is injected by goreleaser via ldflags.
var version = "dev"

// assets holds the templates and static files shipped inside the binary.
//
//go:embed templates/*.html static
var assets embed.FS

func main() {
	api.InitServer(assets, version)
}
