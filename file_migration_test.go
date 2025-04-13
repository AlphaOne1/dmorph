// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"database/sql"
	"io/fs"
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
