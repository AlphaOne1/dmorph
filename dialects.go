// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"database/sql"
	"errors"
	"fmt"
)

type BaseDialect struct {
	CreateTemplate   string
	AppliedTemplate  string
	RegisterTemplate string
}

func (b BaseDialect) EnsureMigrationTableExists(db *sql.DB, tableName string) error {
	tx, err := db.Begin()

	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback() }()

	_, execErr := tx.Exec(fmt.Sprintf(b.CreateTemplate, tableName))

	if execErr != nil {
		rollbackErr := tx.Rollback()
		return errors.Join(execErr, rollbackErr)
	}

	if err := tx.Commit(); err != nil {
		rollbackErr := tx.Rollback()
		return errors.Join(err, rollbackErr)
	}

	return nil
}

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

func (b BaseDialect) RegisterMigration(tx *sql.Tx, id string, tableName string) error {
	_, err := tx.Exec(fmt.Sprintf(b.RegisterTemplate, tableName),
		sql.Named("id", id))

	return err
}
