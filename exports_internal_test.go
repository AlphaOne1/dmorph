package dmorph

import (
	"context"
	"database/sql"
)

// The exported names in this file are only used for internal testing and are not part of the public API.

var (
	TapplyStepsStream    = applyStepsStream
	TmigrationFromFileFS = migrationFromFileFS
	TmigrationOrder      = migrationOrder
)

func (m *Morpher) TapplyMigrations(ctx context.Context, db *sql.DB, lastMigration string) error {
	return m.applyMigrations(ctx, db, lastMigration)
}
