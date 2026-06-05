package api

import "embed"

//go:embed templates/*.html
var embeddedTemplates embed.FS
