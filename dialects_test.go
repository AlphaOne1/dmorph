// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"database/sql"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDialectStatements verifies that each database dialect has valid and
// sufficiently complete SQL statement templates.
func TestDialectStatements(t *testing.T) {
	// we cannot run tests against all databases, but at least we can test
	// that the statements for the databases are somehow filled
	tests := []struct {
		name   string
		caller func() BaseDialect
	}{
		{name: "DB2", caller: DialectDB2},
		{name: "MSSQL", caller: DialectMSSQL},
		{name: "MySQL", caller: DialectMySQL},
		{name: "Oracle", caller: DialectOracle},
		{name: "Postgres", caller: DialectPostgres},
		{name: "SQLite", caller: DialectSQLite},
	}

	re := regexp.MustCompile("%s")

	for k, v := range tests {
		d := v.caller()

		if len(d.CreateTemplate) < 10 {
			t.Errorf("%v: create template is too short for %v", k, v.name)
		}
		assert.Contains(t, d.CreateTemplate, "%s",
			"no table name placeholder in create template for", v.name)
		assert.Regexp(t, re, d.CreateTemplate)

		if len(d.AppliedTemplate) < 10 {
			t.Errorf("%v: applied template is too short for %v", k, v.name)
		}
		assert.Contains(t, d.AppliedTemplate, "%s",
			"no table name placeholder in applied template for", v.name)
		assert.Regexp(t, re, d.AppliedTemplate)

		if len(d.RegisterTemplate) < 10 {
			t.Errorf("%v: register template is too short for %v", k, v.name)
		}
		assert.Contains(t, d.RegisterTemplate, "%s",
			"no table name placeholder in register template for", v.name)
		assert.Regexp(t, re, d.RegisterTemplate)
	}
}

// TestCallsOnClosedDB verifies that methods fail as expected when called on a closed database connection.
func TestCallsOnClosedDB(t *testing.T) {
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

	assert.Error(t,
		DialectSQLite().EnsureMigrationTableExists(db, "irrelevant"),
		"expected error on closed database")

	_, err := DialectSQLite().AppliedMigrations(db, "irrelevant")
	assert.Error(t, err, "expected error on closed database")
}

// TestEnsureMigrationTableExistsSQLError tests the EnsureMigrationTableExists function
// for handling SQL errors during execution.
func TestEnsureMigrationTableExistsSQLError(t *testing.T) {
	d := BaseDialect{
		CreateTemplate: `
            CRATE TABLE test (
                id        VARCHAR(255) PRIMARY KEY,
                create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,
	}

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

	assert.Error(t, d.EnsureMigrationTableExists(db, "test"), "expected error")
}

// TestEnsureMigrationTableExistsCommitError tests the behavior of EnsureMigrationTableExists when a commit error occurs.
func TestEnsureMigrationTableExistsCommitError(t *testing.T) {
	d := BaseDialect{
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

	_, execErr := db.Exec("PRAGMA foreign_keys = ON")

	assert.NoError(t, execErr, "foreign keys checking could not be enabled")

	assert.Error(t, d.EnsureMigrationTableExists(db, "test"), "expected error")
}
