// SPDX-FileCopyrightText: 2025 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph_test

import (
	"bytes"
	"io/fs"
	"log/slog"
	"os"
	"testing"

	"github.com/AlphaOne1/dmorph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithMigrationFromFile(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"))

	assert.NoError(t, runErr, "did not expect an error")
}

func TestWithMigrationFromFileError(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/00_non_existent.sql"))

	var pathErr *fs.PathError
	assert.ErrorAs(t, runErr, &pathErr, "unexpected error")
}

// TestMigrationFromFileFSError validates that migrationFromFileFS returns an error
// when the specified file does not exist.
func TestMigrationFromFileFSError(t *testing.T) {
	t.Parallel()

	dir := os.DirFS("testData")

	mig := dmorph.TmigrationFromFileFS("nonexistent", dir, slog.Default())

	err := mig.Migrate(t.Context(), nil)

	assert.Error(t, err, "expected error")
}

// TestApplyStepsStreamError tests error handling in applyStepsStream.
func TestApplyStepsStreamError(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	buf := bytes.Buffer{}
	buf.WriteString("utter nonsense")

	tx, txErr := db.BeginTx(t.Context(), nil)

	require.NoError(t, txErr, "expected no tx error")

	err := dmorph.TapplyStepsStream(t.Context(), tx, &buf, "test", slog.Default())

	require.Error(t, err, "expected error")

	_ = tx.Rollback()

	tx, txErr = db.BeginTx(t.Context(), nil)

	require.NoError(t, txErr, "expected no tx error")

	buf.Reset()
	buf.WriteString("utter nonsense\n;")

	err = dmorph.TapplyStepsStream(t.Context(), tx, &buf, "test", slog.Default())

	require.Error(t, err, "expected error")

	_ = tx.Rollback()
}
