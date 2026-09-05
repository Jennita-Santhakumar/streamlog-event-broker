// Package migrations embeds the SQL migration files into the binary so the
// migrate command works the same way whether run locally or inside a
// container image (no need to separately COPY a migrations/ directory).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
