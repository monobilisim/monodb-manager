package main

import (
	"github.com/monobilisim/monodb-manager/api"
)

// Build-time variables injected by goreleaser via ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	_ = version
	_ = commit
	_ = date
	api.InitServer()
}
