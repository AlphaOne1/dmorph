// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"database/sql"
	"errors"
	"fmt"
)

// BaseDialect is a convenience type for databases that manage the necessary operations solely using
// queries. Defining the CreateTemplate, AppliedTemplate and RegisterTemplate enables the BaseDialect to
// perform all the necessary operations to fulfill the Dialect interface.
type BaseDialect struct {
	CreateTemplate   string // statement ensuring the existence of the migration table
	AppliedTemplate  string // statement getting applied migrations ordered by application date
	RegisterTemplate string // statement registering a migration
}

// EnsureMigrationTableExists ensures that the migration table, saving the applied migrations ids, exists.
func (b BaseDialect) EnsureMigrationTableExists(db *sql.DB, tableName string) error {
	tx, err := db.Begin()

	if err != nil {
		return err
	}

	// Safety net for unexpected panics
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	if _, execErr := tx.Exec(fmt.Sprintf(b.CreateTemplate, tableName)); execErr != nil {
		rollbackErr := tx.Rollback()
		tx = nil

		return errors.Join(execErr, rollbackErr)
	}

	if err := tx.Commit(); err != nil {
		rollbackErr := tx.Rollback()
		tx = nil

		return errors.Join(err, rollbackErr)
	}

	tx = nil

	return nil
}

// AppliedMigrations gets the already applied migrations from the database, ordered by application date.
func (b BaseDialect) AppliedMigrations(db *sql.DB, tableName string) ([]string, error) {
	rows, rowsErr := db.Query(fmt.Sprintf(b.AppliedTemplate, tableName))

	if rowsErr != nil {
		return nil, rowsErr
	}

	defer func() { _ = rows.Close() }()

	var result []string
	var tmp string
	var scanErr error

	for rows.Next() && scanErr == nil {
		if scanErr = rows.Scan(&tmp); scanErr == nil {
			result = append(result, tmp)
		}
	}

	return result, errors.Join(rows.Err(), scanErr)
}

// RegisterMigration registers a migration in the migration table.
func (b BaseDialect) RegisterMigration(tx *sql.Tx, id string, tableName string) error {
	_, err := tx.Exec(fmt.Sprintf(b.RegisterTemplate, tableName),
		sql.Named("id", id))

	return err
}
