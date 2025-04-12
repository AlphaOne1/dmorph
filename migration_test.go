// Copyright the dmorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"database/sql"
	"embed"
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "modernc.org/sqlite"
)

//go:embed testData
var testMigrationsDir embed.FS

func prepareDB() (string, error) {
	var result string

	dbFile, dbFileErr := os.CreateTemp("", "")

	if dbFileErr != nil {
		return "", dbFileErr
	}

	result = dbFile.Name()

	_ = dbFile.Close()

	return result, nil
}

// TestMigration tests the happy flow.
func TestMigration(t *testing.T) {
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

	migrationsDir, migrationsDirErr := fs.Sub(testMigrationsDir, "testData")

	assert.NoError(t, migrationsDirErr, "migrations directory could not be opened")

	runErr := Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationsFromFS(migrationsDir.(fs.ReadDirFS)))

	if runErr != nil {
		t.Errorf("Migrations could not be run: %v", runErr)
	}
}

// TestMigration tests what happens, if the applied migrations are too old.
func TestMigrationTooOld(t *testing.T) {
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

	migrationsDir, migrationsDirErr := fs.Sub(testMigrationsDir, "testData")

	assert.NoError(t, migrationsDirErr, "migrations directory could not be opened")

	runErr := Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationsFromFS(migrationsDir.(fs.ReadDirFS)))

	if runErr != nil {
		t.Errorf("preparation migrations could not be run: %v", runErr)
	}

	runErr = Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationFromFileFS("01_base_table.sql", migrationsDir))

	assert.ErrorIs(t, runErr, ErrMigrationsTooOld, "migrations did not give expected error")
}

// TestMigration tests what happens, if the applied migrations are unrelated to existing ones.
func TestMigrationUnrelated(t *testing.T) {
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

	migrationsDir, migrationsDirErr := fs.Sub(testMigrationsDir, "testData")

	assert.NoError(t, migrationsDirErr, "migrations directory could not be opened")

	runErr := Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationFromFileFS("01_base_table.sql", migrationsDir))

	if runErr != nil {
		t.Errorf("preparation migrations could not be run: %v", runErr)
	}

	runErr = Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationFromFileFS("02_addon_table.sql", migrationsDir))

	assert.ErrorIs(t, runErr, ErrMigrationsUnrelated, "migrations did not give expected error")
}
