// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"context"
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
func (b BaseDialect) EnsureMigrationTableExists(ctx context.Context, db *sql.DB, tableName string) error {
	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return wrapIfError("could not start transaction", err)
	}

	// Safety net for unexpected panics
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	if _, execErr := tx.ExecContext(ctx, fmt.Sprintf(b.CreateTemplate, tableName)); execErr != nil {
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
func (b BaseDialect) AppliedMigrations(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	rows, rowsErr := db.QueryContext(ctx, fmt.Sprintf(b.AppliedTemplate, tableName))

	if rowsErr != nil {
		return nil, wrapIfError("could not get applied migrations", rowsErr)
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
func (b BaseDialect) RegisterMigration(ctx context.Context, tx *sql.Tx, id string, tableName string) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf(b.RegisterTemplate, tableName),
		sql.Named("id", id))

	return wrapIfError("could not register migration", err)
}
