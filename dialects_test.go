// SPDX-FileCopyrightText: 2025 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AlphaOne1/dmorph"
)

// TestDialectStatements verifies that each database dialect has valid and
// sufficiently complete SQL statement templates.
func TestDialectStatements(t *testing.T) {
	t.Parallel()

	// we cannot run tests against all databases, but at least we can test
	// that the statements for the databases are somehow filled
	tests := []struct {
		name   string
		caller func() dmorph.BaseDialect
	}{
		{name: "CSVQ", caller: dmorph.DialectCSVQ},
		{name: "DB2", caller: dmorph.DialectDB2},
		{name: "MSSQL", caller: dmorph.DialectMSSQL},
		{name: "MySQL", caller: dmorph.DialectMySQL},
		{name: "Oracle", caller: dmorph.DialectOracle},
		{name: "Postgres", caller: dmorph.DialectPostgres},
		{name: "SQLite", caller: dmorph.DialectSQLite},
	}

	for k, test := range tests {
		t.Run(fmt.Sprintf("TestDialectStatements-%d", k), func(t *testing.T) {
			t.Parallel()

			dialect := test.caller()

			if len(dialect.CreateTemplate) < 10 {
				t.Errorf("create template is too short for %v", test.name)
			}
			assert.Contains(t, dialect.CreateTemplate, "%s",
				"no table name placeholder in create template for", test.name)

			if len(dialect.AppliedTemplate) < 10 {
				t.Errorf("applied template is too short for %v", test.name)
			}
			assert.Contains(t, dialect.AppliedTemplate, "%s",
				"no table name placeholder in applied template for", test.name)

			if len(dialect.RegisterTemplate) < 10 {
				t.Errorf("register template is too short for %v", test.name)
			}
			assert.Contains(t, dialect.RegisterTemplate, "%s",
				"no table name placeholder in register template for", test.name)
		})
	}
}

// TestCallsOnClosedDB verifies that methods fail as expected when called on a closed database connection.
func TestCallsOnClosedDB(t *testing.T) {
	t.Parallel()

	db := openTempSQLite(t)
	require.NoError(t, db.Close())

	require.Error(t,
		dmorph.DialectSQLite().EnsureMigrationTableExists(t.Context(), db, "irrelevant"),
		"expected error on closed database")

	_, err := dmorph.DialectSQLite().AppliedMigrations(t.Context(), db, "irrelevant")
	require.Error(t, err, "expected error on closed database")
}

// TestEnsureMigrationTableExistsSQLError tests the EnsureMigrationTableExists function
// for handling SQL errors during execution.
func TestEnsureMigrationTableExistsSQLError(t *testing.T) {
	t.Parallel()

	dialect := dmorph.BaseDialect{
		CreateTemplate: `
            CRATE TABLE test (
                id        VARCHAR(255) PRIMARY KEY,
                create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,
	}

	db := openTempSQLite(t)

	assert.Error(t, dialect.EnsureMigrationTableExists(t.Context(), db, "test"), "expected error")
}

// TestEnsureMigrationTableExistsCommitError tests the behavior of EnsureMigrationTableExists
// when a commit error occurs.
func TestEnsureMigrationTableExistsCommitError(t *testing.T) {
	t.Parallel()

	dialect := dmorph.BaseDialect{
		CreateTemplate: `
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
			DELETE FROM t0 WHERE id = 1;`,
	}

	db := openTempSQLite(t)

	_, execErr := db.ExecContext(t.Context(), "PRAGMA foreign_keys = ON")

	require.NoError(t, execErr, "foreign keys checking could not be enabled")

	assert.Error(t, dialect.EnsureMigrationTableExists(t.Context(), db, "test"), "expected error")
}
