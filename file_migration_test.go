// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"bytes"
	"database/sql"
	"io/fs"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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

	runErr := Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationFromFile("testData/01_base_table.sql"))

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

	runErr := Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationFromFile("testData/00_non_existent.sql"))

	var pathErr *fs.PathError
	assert.ErrorAs(t, runErr, &pathErr, "unexpected error")
}

// TestMigrationFromFileFSError validates that migrationFromFileFS returns an error when the specified file does not exist.
func TestMigrationFromFileFSError(t *testing.T) {
	dir, dirErr := os.OpenRoot("testData")

	assert.NoError(t, dirErr, "could not open test data directory")

	mig := migrationFromFileFS("nonexistent", dir.FS(), slog.Default())

	err := mig.Migrate(nil)

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

	tx, txErr := db.Begin()

	assert.NoError(t, txErr, "expected no tx error")

	err := applyStepsStream(tx, &buf, "test", slog.Default())

	assert.Error(t, err, "expected error")

	_ = tx.Rollback()

	tx, txErr = db.Begin()

	assert.NoError(t, txErr, "expected no tx error")

	buf.Reset()
	buf.WriteString("utter nonsense\n;")

	err = applyStepsStream(tx, &buf, "test", slog.Default())

	assert.Error(t, err, "expected error")

	_ = tx.Rollback()
}
