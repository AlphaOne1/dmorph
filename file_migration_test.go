// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph_test

import (
	"bytes"
	"database/sql"
	"io/fs"
	"log/slog"
	"os"
	"testing"

	"github.com/AlphaOne1/dmorph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithMigrationFromFile(t *testing.T) {
	dbFile, dbFileErr := prepareDB()

	if dbFileErr != nil {
		t.Errorf("DB file could not be created: %v", dbFileErr)
	} else {
		defer func() { _ = os.Remove(dbFile) }()
	}

	db, dbErr := sql.Open("sqlite", dbFile)

	if dbErr != nil {
		t.Errorf("DB file could not be created: %v", dbErr)
	} else {
		defer func() { _ = db.Close() }()
	}

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"))

	assert.NoError(t, runErr, "did not expect an error")
}

func TestWithMigrationFromFileError(t *testing.T) {
	dbFile, dbFileErr := prepareDB()

	if dbFileErr != nil {
		t.Errorf("DB file could not be created: %v", dbFileErr)
	} else {
		defer func() { _ = os.Remove(dbFile) }()
	}

	db, dbErr := sql.Open("sqlite", dbFile)

	if dbErr != nil {
		t.Errorf("DB file could not be created: %v", dbErr)
	} else {
		defer func() { _ = db.Close() }()
	}

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
	dir := os.DirFS("testData")

	mig := dmorph.TmigrationFromFileFS("nonexistent", dir, slog.Default())

	err := mig.Migrate(t.Context(), nil)

	assert.Error(t, err, "expected error")
}

// TestApplyStepsStreamError tests error handling in applyStepsStream.
func TestApplyStepsStreamError(t *testing.T) {
	dbFile, dbFileErr := prepareDB()

	if dbFileErr != nil {
		t.Errorf("DB file could not be created: %v", dbFileErr)
	} else {
		defer func() { _ = os.Remove(dbFile) }()
	}

	db, dbErr := sql.Open("sqlite", dbFile)

	if dbErr != nil {
		t.Errorf("DB file could not be created: %v", dbErr)
	} else {
		defer func() { _ = db.Close() }()
	}

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

	assert.Error(t, err, "expected error")

	_ = tx.Rollback()
}
