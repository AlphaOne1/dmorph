// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

	for k, v := range tests {
		d := v.caller()

		if len(d.CreateTemplate) < 10 {
			t.Errorf("%v: create template is too short for %v", k, v.name)
		}
		assert.Contains(t, d.CreateTemplate, "%s",
			"no table name placeholder in create template for", v.name)

		if len(d.AppliedTemplate) < 10 {
			t.Errorf("%v: applied template is too short for %v", k, v.name)
		}
		assert.Contains(t, d.AppliedTemplate, "%s",
			"no table name placeholder in applied template for", v.name)

		if len(d.RegisterTemplate) < 10 {
			t.Errorf("%v: register template is too short for %v", k, v.name)
		}
		assert.Contains(t, d.RegisterTemplate, "%s",
			"no table name placeholder in register template for", v.name)
	}
}

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
		// defer func() { _ = os.Remove(dbFile) }()
	}

	db, dbErr := sql.Open("sqlite", "file://"+dbFile+"?_pragma=foreign_keys(1)")

	if dbErr != nil {
		t.Errorf("DB file could not be created: %v", dbErr)
	} else {
		defer func() { _ = db.Close() }()
	}

	assert.Error(t, d.EnsureMigrationTableExists(db, "test"), "expected error")
}
