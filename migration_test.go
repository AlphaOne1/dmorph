// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"database/sql"
	"embed"
	"io/fs"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "modernc.org/sqlite"
)

//go:embed testData
var testMigrationsDir embed.FS

// prepareDB creates a temporary SQLite database file and returns its file path.
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

type TestMigrationImpl struct{}

func (m TestMigrationImpl) Key() string { return "TestMigration" }
func (m TestMigrationImpl) Migrate(tx *sql.Tx) error {
	_, err := tx.Exec("CREATE TABLE t0 (id INTEGER PRIMARY KEY)")
	return err
}

// TestWithMigrations tests the adding of migrations using WithMigrations.
func TestWithMigrations(t *testing.T) {
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
		WithMigrations(TestMigrationImpl{}))

	assert.NoError(t, runErr, "did not expect error")
}

// TestMigrationUnableToCreateMorpher tests to use the Run function without any
// useful parameter.
func TestMigrationUnableToCreateMorpher(t *testing.T) {
	runErr := Run(nil)

	assert.Error(t, runErr, "morpher should not have run")
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

// TestMigrationWithLogger validates the creation of a Morpher with a logger and ensures
// the logger is applied correctly.
func TestMigrationWithLogger(t *testing.T) {
	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	morpher, err := NewMorpher(
		WithDialect(DialectSQLite()),
		WithMigrationFromFile("testData/01_base_table.sql"),
		WithLog(l),
	)

	assert.NoError(t, err, "morpher could not be created")
	assert.Equal(t, l, morpher.Log, "logger was not set correctly")
}

// TestMigrationWithoutMigrations ensures that creating a Morpher instance without migrations results in an error.
func TestMigrationWithoutMigrations(t *testing.T) {
	_, err := NewMorpher(
		WithDialect(DialectSQLite()),
	)

	assert.Error(t, err, "morpher created without migrations")
}

// TestMigrationWithTableNameValid verifies the correct creation of a Morpher
// with a valid custom table name configuration.
func TestMigrationWithTableNameValid(t *testing.T) {
	morpher, err := NewMorpher(
		WithDialect(DialectSQLite()),
		WithMigrationFromFile("testData/01_base_table.sql"),
		WithTableName("dimorphodon"),
	)

	assert.NoError(t, err, "morpher could not be created")
	assert.Equal(t, "dimorphodon", morpher.TableName, "table name was not set correctly")
}

// TestMigrationWithTableNameInvalidSize verifies that creating a Morpher
// with an invalid table name size produces an error.
func TestMigrationWithTableNameInvalidSize(t *testing.T) {
	_, err := NewMorpher(
		WithDialect(DialectSQLite()),
		WithMigrationFromFile("testData/01_base_table.sql"),
		WithTableName(""),
	)

	assert.Error(t, err, "morpher could created with empty table name")
}

// TestMigrationWithTableNameInvalidChars ensures that creating a Morpher
// fails when the table name contains invalid characters.
func TestMigrationWithTableNameInvalidChars(t *testing.T) {
	_, err := NewMorpher(
		WithDialect(DialectSQLite()),
		WithMigrationFromFile("testData/01_base_table.sql"),
		WithTableName("di/mor/pho/don"),
	)

	assert.Error(t, err, "morpher could created with invalid table name")
}

// TestMigrationRunInvalid verifies that running a Morpher with invalid configuration results in an error.
func TestMigrationRunInvalid(t *testing.T) {
	morpher := Morpher{}

	runErr := morpher.Run(nil)

	assert.Error(t, runErr, "morpher should run")
}

// TestMigrationRunInvalidCreate tests the behavior of running a migration
// with an invalid CreateTemplate in the dialect.
func TestMigrationRunInvalidCreate(t *testing.T) {
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

	dialect := DialectSQLite()
	dialect.CreateTemplate = "utter nonsense"

	morpher, morpherErr := NewMorpher(
		WithDialect(dialect),
		WithMigrationFromFile("testData/01_base_table.sql"))

	assert.NoError(t, morpherErr, "morpher could not be created")

	runErr := morpher.Run(db)

	assert.Error(t, runErr, "morpher should not run")
}

// TestMigrationRunInvalidApplied tests the failure scenario where the AppliedTemplate of the dialect is invalid.
func TestMigrationRunInvalidApplied(t *testing.T) {
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

	dialect := DialectSQLite()
	dialect.AppliedTemplate = "utter nonsense"

	morpher, morpherErr := NewMorpher(
		WithDialect(dialect),
		WithMigrationFromFile("testData/01_base_table.sql"))

	assert.NoError(t, morpherErr, "morpher could not be created")

	runErr := morpher.Run(db)

	assert.Error(t, runErr, "morpher should not run")
}

// TestMigrationApplyInvalidDB verifies that applying migrations to an invalid or closed database results in an error.
func TestMigrationApplyInvalidDB(t *testing.T) {
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
		_ = db.Close()
	}

	morpher, morpherErr := NewMorpher(
		WithDialect(DialectSQLite()),
		WithMigrationFromFile("testData/01_base_table.sql"))

	assert.NoError(t, morpherErr, "morpher could not be created")

	assert.Error(t,
		morpher.applyMigrations(db, "irrelevant"),
		"morpher should error on invalid DB")
}

// TestMigrationApplyUnableRegister tests the behavior when the migration registration fails due to an invalid template.
func TestMigrationApplyUnableRegister(t *testing.T) {
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

	morpher, morpherErr := NewMorpher(
		WithDialect(DialectSQLite()),
		WithMigrationFromFile("testData/01_base_table.sql"))

	assert.NoError(t, morpherErr, "morpher could not be created")

	d, _ := morpher.Dialect.(BaseDialect)
	d.RegisterTemplate = "utter nonsense"
	morpher.Dialect = d

	assert.Error(t,
		morpher.applyMigrations(db, ""),
		"morpher should fail to register")
}

// TestMigrationApplyUnableCommit tests the scenario where migration application fails
// due to inability to commit a transaction.
func TestMigrationApplyUnableCommit(t *testing.T) {
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

	morpher, morpherErr := NewMorpher(
		WithDialect(DialectSQLite()),
		WithMigrationFromFile("testData/01_base_table.sql"))

	assert.NoError(t, morpherErr, "morpher could not be created")

	_, execErr := db.Exec("PRAGMA foreign_keys = ON")
	assert.NoError(t, execErr, "foreign keys checking could not be enabled")

	d, _ := morpher.Dialect.(BaseDialect)
	d.RegisterTemplate = `
		CREATE TABLE t0 (
			id INTEGER PRIMARY KEY
		);

		CREATE TABLE t1 (
			id        INTEGER PRIMARY KEY,
			parent_id INTEGER REFERENCES t0 (id) DEFERRABLE INITIALLY DEFERRED
		);

		INSERT INTO t0 (id)            VALUES (1);
		INSERT INTO t1 (id, parent_id) VALUES (1, 1);

		-- %s catching argument
		DELETE FROM t0 WHERE id = 1;`

	morpher.Dialect = d

	assert.Error(t,
		morpher.applyMigrations(db, ""),
		"morpher should fail to register")
}
