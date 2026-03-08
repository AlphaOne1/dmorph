// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph_test

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/AlphaOne1/dmorph"
)

//go:embed testData
var testMigrationsDir embed.FS

// openTempSQLite opens a temporary in-memory SQLite database for testing and ensures it is closed after the test ends.
func openTempSQLite(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err, "DB could not be opened")
	t.Cleanup(func() { _ = db.Close() })

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return db
}

// TestMigration tests the happy flow.
func TestMigration(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	migrationsDir, migrationsDirErr := fs.Sub(testMigrationsDir, "testData")

	require.NoError(t, migrationsDirErr, "migrations directory could not be opened")

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationsFromFS(migrationsDir))

	assert.NoError(t, runErr, "migrations could not be run")
}

// TestMigrationUpdate tests the happy flow of updating on existing migrations.
func TestMigrationUpdate(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	migrationsDir, migrationsDirErr := fs.Sub(testMigrationsDir, "testData")

	require.NoError(t, migrationsDirErr, "migrations directory could not be opened")

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFileFS("01_base_table.sql", migrationsDir))

	require.NoError(t, runErr, "preparation migrations could not be run")

	runErr = dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationsFromFS(migrationsDir))

	assert.NoError(t, runErr, "migrations could not be run")
}

type TestMigrationImpl struct{}

func (m TestMigrationImpl) Key() string { return "TestMigration" }
func (m TestMigrationImpl) Migrate(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "CREATE TABLE t0 (id INTEGER PRIMARY KEY)")

	return dmorph.TwrapIfError("could not migrate", err) //nolint:wrapcheck
}

// TestWithMigrations tests the adding of migrations using WithMigrations.
func TestWithMigrations(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrations(TestMigrationImpl{}))

	assert.NoError(t, runErr, "did not expect error")
}

// TestMigrationUnableToCreateMorpher tests to use the Run function without any
// useful parameter.
func TestMigrationUnableToCreateMorpher(t *testing.T) {
	t.Parallel()

	runErr := dmorph.Run(t.Context(), nil)

	assert.Error(t, runErr, "morpher should not have run")
}

// TestMigrationTooOld tests what happens if the applied migrations are too old.
func TestMigrationTooOld(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	migrationsDir, migrationsDirErr := fs.Sub(testMigrationsDir, "testData")

	require.NoError(t, migrationsDirErr, "migrations directory could not be opened")

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationsFromFS(migrationsDir))

	require.NoError(t, runErr, "preparation migrations could not be run")

	runErr = dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFileFS("01_base_table.sql", migrationsDir))

	assert.ErrorIs(t, runErr, dmorph.ErrMigrationsTooOld, "migrations did not give expected error")
}

// TestMigrationUnrelated0 tests what happens if the applied migrations are unrelated to existing ones.
func TestMigrationUnrelated0(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	migrationsDir, migrationsDirErr := fs.Sub(testMigrationsDir, "testData")

	require.NoError(t, migrationsDirErr, "migrations directory could not be opened")

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationsFromFS(migrationsDir))

	require.NoError(t, runErr, "preparation migrations could not be run")

	runErr = dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFileFS("02_addon_table.sql", migrationsDir))

	assert.ErrorIs(t, runErr, dmorph.ErrMigrationsUnrelated, "migrations did not give expected error")
}

// TestMigrationUnrelated1 tests what happens if the applied migrations are unrelated to existing ones.
func TestMigrationUnrelated1(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	migrationsDir, migrationsDirErr := fs.Sub(testMigrationsDir, "testData")

	require.NoError(t, migrationsDirErr, "migrations directory could not be opened")

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFileFS("01_base_table.sql", migrationsDir))

	require.NoError(t, runErr, "preparation migrations could not be run")

	runErr = dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFileFS("02_addon_table.sql", migrationsDir))

	assert.ErrorIs(t, runErr, dmorph.ErrMigrationsUnrelated, "migrations did not give expected error")
}

// TestMigrationAppliedUnordered tests the case, that somehow the migrations in the
// database are registered not in the order of their keys.
func TestMigrationAppliedUnordered(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	migrationsDir, migrationsDirErr := fs.Sub(testMigrationsDir, "testData")

	require.NoError(t, migrationsDirErr, "migrations directory could not be opened")

	require.NoError(t, dmorph.DialectSQLite().EnsureMigrationTableExists(t.Context(), db, "migrations"))

	_, execErr := db.ExecContext(t.Context(), `
		INSERT INTO migrations (id, create_ts) VALUES ('01_base_table',  '2021-01-02 00:00:00');
		INSERT INTO migrations (id, create_ts) VALUES ('02_addon_table', '2021-01-01 00:00:00');
	`)

	require.NoError(t, execErr, "unordered test could not be prepared")

	runErr := dmorph.Run(t.Context(),
		db,
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationsFromFS(migrationsDir))

	assert.ErrorIs(t,
		runErr,
		dmorph.ErrMigrationsUnsorted,
		"migrations did not give expected error")
}

// TestMigrationOrder checks that the migrations ordering function works as expected.
func TestMigrationOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		m0    dmorph.Migration
		m1    dmorph.Migration
		order int
	}{
		{
			m0:    dmorph.FileMigration{Name: "01"},
			m1:    dmorph.FileMigration{Name: "01"},
			order: 0,
		},
		{
			m0:    dmorph.FileMigration{Name: "01"},
			m1:    dmorph.FileMigration{Name: "02"},
			order: -1,
		},
		{
			m0:    dmorph.FileMigration{Name: "02"},
			m1:    dmorph.FileMigration{Name: "01"},
			order: 1,
		},
	}

	for k, v := range tests {
		t.Run(fmt.Sprintf("TestMigrationOrder %v", k), func(t *testing.T) {
			t.Parallel()

			res := dmorph.TmigrationOrder(v.m0, v.m1)

			assert.Equal(t, v.order, res, "order of migrations is wrong for test %v", k)
		})
	}
}

// TestMigrationIsValid checks the validity checks for migrations.
func TestMigrationIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		m   dmorph.Morpher
		err error
	}{
		{
			m: dmorph.Morpher{
				Dialect:    dmorph.DialectSQLite(),
				Migrations: []dmorph.Migration{dmorph.FileMigration{Name: "01"}},
				TableName:  "migrations",
			},
			err: nil,
		},
		{
			m: dmorph.Morpher{
				Dialect:    nil,
				Migrations: []dmorph.Migration{dmorph.FileMigration{Name: "01"}},
				TableName:  "migrations",
			},
			err: dmorph.ErrNoDialect,
		},
		{
			m: dmorph.Morpher{
				Dialect:    dmorph.DialectSQLite(),
				Migrations: nil,
				TableName:  "migrations",
			},
			err: dmorph.ErrNoMigrations,
		},
		{
			m: dmorph.Morpher{
				Dialect:    dmorph.DialectSQLite(),
				Migrations: []dmorph.Migration{dmorph.FileMigration{Name: "01"}},
				TableName:  "",
			},
			err: dmorph.ErrNoMigrationTable,
		},
		{
			m: dmorph.Morpher{
				Dialect:    dmorph.DialectSQLite(),
				Migrations: []dmorph.Migration{dmorph.FileMigration{Name: "01"}},
				TableName:  "blah(); DROP TABLE blah;",
			},
			err: dmorph.ErrMigrationTableNameInvalid,
		},
	}

	for k, v := range tests {
		t.Run(fmt.Sprintf("TestMigrationIsValid %v", k), func(t *testing.T) {
			t.Parallel()

			err := v.m.IsValid()

			assert.ErrorIs(t, err, v.err, "error is wrong for test %v", k)
		})
	}
}

// TestMigrationWithLogger validates the creation of a Morpher with a logger and ensures
// the logger is applied correctly.
func TestMigrationWithLogger(t *testing.T) {
	t.Parallel()

	newLog := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	morpher, err := dmorph.NewMorpher(
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"),
		dmorph.WithLog(newLog),
	)

	require.NoError(t, err, "morpher could not be created")
	assert.Equal(t, newLog, morpher.Log, "logger was not set correctly")
}

// TestMigrationWithoutMigrations ensures that creating a Morpher instance without migrations results in an error.
func TestMigrationWithoutMigrations(t *testing.T) {
	t.Parallel()

	_, err := dmorph.NewMorpher(
		dmorph.WithDialect(dmorph.DialectSQLite()),
	)

	assert.Error(t, err, "morpher created without migrations")
}

// TestMigrationWithTableNameValid verifies the correct creation of a Morpher
// with a valid custom table name configuration.
func TestMigrationWithTableNameValid(t *testing.T) {
	t.Parallel()

	morpher, err := dmorph.NewMorpher(
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"),
		dmorph.WithTableName("dimorphodon"),
	)

	require.NoError(t, err, "morpher could not be created")
	assert.Equal(t, "dimorphodon", morpher.TableName, "table name was not set correctly")
}

// TestMigrationWithTableNameInvalidSize verifies that creating a Morpher
// with an invalid table name size produces an error.
func TestMigrationWithTableNameInvalidSize(t *testing.T) {
	t.Parallel()

	_, err := dmorph.NewMorpher(
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"),
		dmorph.WithTableName(""),
	)

	assert.Error(t, err, "morpher could be created with empty table name")
}

// TestMigrationWithTableNameInvalidChars ensures that creating a Morpher
// fails when the table name contains invalid characters.
func TestMigrationWithTableNameInvalidChars(t *testing.T) {
	t.Parallel()

	_, err := dmorph.NewMorpher(
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"),
		dmorph.WithTableName("di/mor/pho/don"),
	)

	assert.Error(t, err, "morpher could be created with invalid table name")
}

// TestMigrationRunInvalid verifies that running a Morpher with invalid configuration results in an error.
func TestMigrationRunInvalid(t *testing.T) {
	t.Parallel()

	morpher := dmorph.Morpher{}

	runErr := morpher.Run(t.Context(), nil)

	assert.Error(t, runErr, "morpher should not run")
}

// TestMigrationRunInvalidCreate tests the behavior of running a migration
// with an invalid CreateTemplate in the dialect.
func TestMigrationRunInvalidCreate(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	dialect := dmorph.DialectSQLite()
	dialect.CreateTemplate = "utter nonsense 0"

	morpher, morpherErr := dmorph.NewMorpher(
		dmorph.WithDialect(dialect),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"))

	require.NoError(t, morpherErr, "morpher could not be created")

	runErr := morpher.Run(t.Context(), db)

	assert.Error(t, runErr, "morpher should not run")
}

// TestMigrationRunInvalidApplied tests the failure scenario where the AppliedTemplate of the dialect is invalid.
func TestMigrationRunInvalidApplied(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	dialect := dmorph.DialectSQLite()
	dialect.AppliedTemplate = "utter nonsense 1"

	morpher, morpherErr := dmorph.NewMorpher(
		dmorph.WithDialect(dialect),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"))

	require.NoError(t, morpherErr, "morpher could not be created")

	runErr := morpher.Run(t.Context(), db)

	assert.Error(t, runErr, "morpher should not run")
}

// TestMigrationApplyInvalidDB verifies that applying migrations to an invalid or closed database results in an error.
func TestMigrationApplyInvalidDB(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	morpher, morpherErr := dmorph.NewMorpher(
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"))

	require.NoError(t, morpherErr, "morpher could not be created")

	assert.Error(t,
		morpher.TapplyMigrations(t.Context(), db, "irrelevant"),
		"morpher should error on invalid DB")
}

// TestMigrationApplyUnableRegister tests the behavior when the migration registration fails due to an invalid template.
func TestMigrationApplyUnableRegister(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	morpher, morpherErr := dmorph.NewMorpher(
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"))

	require.NoError(t, morpherErr, "morpher could not be created")

	d, dialectOK := morpher.Dialect.(dmorph.BaseDialect)
	require.True(t, dialectOK, "dialect is not a BaseDialect")

	d.RegisterTemplate = "utter nonsense 2"
	morpher.Dialect = d

	assert.Error(t,
		morpher.TapplyMigrations(t.Context(), db, ""),
		"morpher should fail to register")
}

// TestMigrationApplyUnableCommit tests the scenario where a migration application fails
// due to inability to commit a transaction.
func TestMigrationApplyUnableCommit(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)

	morpher, morpherErr := dmorph.NewMorpher(
		dmorph.WithDialect(dmorph.DialectSQLite()),
		dmorph.WithMigrationFromFile("testData/01_base_table.sql"))

	require.NoError(t, morpherErr, "morpher could not be created")

	_, execErr := db.ExecContext(t.Context(), "PRAGMA foreign_keys = ON")
	require.NoError(t, execErr, "foreign keys checking could not be enabled")

	baseDialect, dialectOK := morpher.Dialect.(dmorph.BaseDialect)
	require.True(t, dialectOK, "dialect is not a BaseDialect")

	baseDialect.RegisterTemplate = `
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

	morpher.Dialect = baseDialect

	assert.Error(t,
		morpher.TapplyMigrations(t.Context(), db, ""),
		"morpher should fail to register")
}

type okDialect struct{}

func (okDialect) EnsureMigrationTableExists(
	_ /* ctx */ context.Context,
	_ /* db */ *sql.DB,
	_ /* tableName */ string) error {

	return nil
}

func (okDialect) AppliedMigrations(
	_ /* ctx */ context.Context,
	_ /* db */ *sql.DB,
	_ /* tableName */ string) ([]string, error) {

	return nil, nil
}

func (okDialect) RegisterMigration(
	_ /* ctx */ context.Context,
	_ /* tx */ *sql.Tx,
	_ /* id */ string,
	_ /* tableName */ string) error {

	return nil
}

type oneMigration struct {
	key string
}

func (m oneMigration) Key() string {
	return m.key
}

func (m oneMigration) Migrate(_ /* ctx */ context.Context, _ /* tx */ *sql.Tx) error {
	return nil
}

func TestRunOneMigrationFailsOnClosedDB(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")

	require.NoError(t, err)
	require.NoError(t, db.Close())

	logger := slog.New(slog.DiscardHandler)

	err = dmorph.Run(
		context.Background(),
		db,
		dmorph.WithDialect(okDialect{}),
		dmorph.WithMigrations(oneMigration{key: "001_test"}),
		dmorph.WithLog(logger),
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "begin tx")
}

func TestApplyFailsOnCanceledContext(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")

	require.NoError(t, err)

	logger := slog.New(slog.DiscardHandler)
	ctx, ctxCancel := context.WithCancel(context.Background())
	ctxCancel()

	err = dmorph.Run(
		ctx,
		db,
		dmorph.WithDialect(okDialect{}),
		dmorph.WithMigrations(oneMigration{key: "001_test"}),
		dmorph.WithLog(logger),
	)

	require.NoError(t, db.Close())
	require.Error(t, err)
	require.ErrorContains(t, err, "context cancelled")
}
