// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"context"
	"database/sql"
)

// The exported names in this file are only used for internal testing and are not part of the public API.

//nolint:gochecknoglobals // these are used for testing and not visible or used otherwise
var (
	TapplyStepsStream    = applyStepsStream
	TmigrationFromFileFS = migrationFromFileFS
	TmigrationOrder      = migrationOrder
	TwrapIfError         = wrapIfError
)

func (m *Morpher) TapplyMigrations(ctx context.Context, db *sql.DB, lastMigration string) error {
	return m.applyMigrations(ctx, db, lastMigration)
}
