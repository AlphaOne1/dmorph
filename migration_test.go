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
	// dbFile, dbFileErr := os.Create("testdb.sqlite")

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

	assert.NoError(t, runErr, "migrations could not be run")
}

// TestMigrationUpdate tests the happy flow of updating on existing migrations.
func TestMigrationUpdate(t *testing.T) {
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

	assert.NoError(t, runErr, "preparation migrations could not be run")

	runErr = Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationsFromFS(migrationsDir.(fs.ReadDirFS)))

	assert.NoError(t, runErr, "migrations could not be run")
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

	assert.NoError(t, runErr, "preparation migrations could not be run")

	runErr = Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationFromFileFS("01_base_table.sql", migrationsDir))

	assert.ErrorIs(t, runErr, ErrMigrationsTooOld, "migrations did not give expected error")
}

// TestMigrationUnrelated0 tests what happens, if the applied migrations are unrelated to existing ones.
func TestMigrationUnrelated0(t *testing.T) {
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

	assert.NoError(t, runErr, "preparation migrations could not be run")

	runErr = Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationFromFileFS("02_addon_table.sql", migrationsDir))

	assert.ErrorIs(t, runErr, ErrMigrationsUnrelated, "migrations did not give expected error")
}

// TestMigrationUnrelated1 tests what happens, if the applied migrations are unrelated to existing ones.
func TestMigrationUnrelated1(t *testing.T) {
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

	assert.NoError(t, runErr, "preparation migrations could not be run")

	runErr = Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationFromFileFS("02_addon_table.sql", migrationsDir))

	assert.ErrorIs(t, runErr, ErrMigrationsUnrelated, "migrations did not give expected error")
}

// TestMigrationAppliedUnordered tests the case, that somehow the migrations in the
// database are registered not in the order of their keys.
func TestMigrationAppliedUnordered(t *testing.T) {
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

	assert.NoError(t, DialectSQLite().EnsureMigrationTableExists(db, "migrations"))

	_, execErr := db.Exec(`
		INSERT INTO migrations (id, create_ts) VALUES ('01_base_table',  '2021-01-02 00:00:00');
		INSERT INTO migrations (id, create_ts) VALUES ('02_addon_table', '2021-01-01 00:00:00');
	`)

	assert.NoError(t, execErr, "unordered test could not be prepared")

	runErr := Run(db,
		WithDialect(DialectSQLite()),
		WithMigrationsFromFS(migrationsDir.(fs.ReadDirFS)))

	assert.ErrorIs(t,
		runErr,
		ErrMigrationsUnsorted,
		"migrations did not give expected error")
}

// TestMigrationOrder checks that the migrations ordering function works as expected.
func TestMigrationOrder(t *testing.T) {
	tests := []struct {
		m0    Migration
		m1    Migration
		order int
	}{
		{
			m0:    FileMigration{Name: "01"},
			m1:    FileMigration{Name: "01"},
			order: 0,
		},
		{
			m0:    FileMigration{Name: "01"},
			m1:    FileMigration{Name: "02"},
			order: -1,
		},
		{
			m0:    FileMigration{Name: "02"},
			m1:    FileMigration{Name: "01"},
			order: 1,
		},
	}

	for k, v := range tests {
		res := migrationOrder(v.m0, v.m1)

		assert.Equal(t, v.order, res, "order of migrations is wrong for test %v", k)
	}
}

// TestMigrationIsValid checks the validity checks for migrations.
func TestMigrationIsValid(t *testing.T) {
	tests := []struct {
		m   Morpher
		err error
	}{
		{
			m: Morpher{
				Dialect:    DialectSQLite(),
				Migrations: []Migration{FileMigration{Name: "01"}},
				TableName:  "migrations",
			},
			err: nil,
		},
		{
			m: Morpher{
				Dialect:    nil,
				Migrations: []Migration{FileMigration{Name: "01"}},
				TableName:  "migrations",
			},
			err: ErrNoDialect,
		},
		{
			m: Morpher{
				Dialect:    DialectSQLite(),
				Migrations: nil,
				TableName:  "migrations",
			},
			err: ErrNoMigrations,
		},
		{
			m: Morpher{
				Dialect:    DialectSQLite(),
				Migrations: []Migration{FileMigration{Name: "01"}},
				TableName:  "",
			},
			err: ErrNoMigrationTable,
		},
		{
			m: Morpher{
				Dialect:    DialectSQLite(),
				Migrations: []Migration{FileMigration{Name: "01"}},
				TableName:  "blah(); DROP TABLE blah;",
			},
			err: ErrMigrationTableNameInvalid,
		},
	}

	for k, v := range tests {
		err := v.m.IsValid()

		assert.ErrorIs(t, err, v.err, "error is wrong for test %v", k)
	}
}
