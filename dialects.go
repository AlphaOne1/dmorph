// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// NamedParamsDialect is a convenience type for databases that manage the necessary operations solely using
// queries. Defining the CreateTemplate, AppliedTemplate and RegisterTemplate enables the NamedParamsDialect to
// perform all the necessary operations to fulfill the Dialect interface.
type NamedParamsDialect struct {
	CreateTemplate   string // statement ensuring the existence of the migration table
	AppliedTemplate  string // statement getting applied migrations ordered by application date
	RegisterTemplate string // statement registering a migration
}

// EnsureMigrationTableExists ensures that the migration table, saving the applied migrations ids, exists.
func (b NamedParamsDialect) EnsureMigrationTableExists(ctx context.Context, db *sql.DB, tableName string) error {
	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return wrapIfError("could not start transaction", err)
	}

	// Safety net for unexpected panics or returns. We can always call Rollback,
	// as it does semantically nothing in case of a previous successful commit
	defer func() { _ = tx.Rollback() }()

	if _, execErr := tx.ExecContext(ctx, fmt.Sprintf(b.CreateTemplate, tableName)); execErr != nil {
		rollbackErr := tx.Rollback()

		return errors.Join(execErr, rollbackErr)
	}

	if err := tx.Commit(); err != nil {
		rollbackErr := tx.Rollback()

		return errors.Join(err, rollbackErr)
	}

	return nil
}

// AppliedMigrations gets the already applied migrations from the database, ordered by application date.
func (b NamedParamsDialect) AppliedMigrations(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	groupName string) ([]string, error) {

	rows, rowsErr := db.QueryContext(ctx, fmt.Sprintf(b.AppliedTemplate, tableName),
		sql.Named("mgroup", groupName))

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
func (b NamedParamsDialect) RegisterMigration(
	ctx context.Context,
	tx *sql.Tx,
	id string,
	tableName string,
	groupName string) error {

	_, err := tx.ExecContext(ctx, fmt.Sprintf(b.RegisterTemplate, tableName),
		sql.Named("id", id),
		sql.Named("mgroup", groupName))

	return wrapIfError("could not register migration", err)
}

type ParamName string

const (
	ParamNameID     ParamName = "id"
	ParamNameMGroup ParamName = "mgroup"
)

type NumberedParamsDialect struct {
	NamedParamsDialect

	AppliedMigrationsParamsOrder []ParamName
	RegisterMigrationParamsOrder []ParamName
}

func (b NumberedParamsDialect) EnsureMigrationTableExists(ctx context.Context, db *sql.DB, tableName string) error {
	return b.NamedParamsDialect.EnsureMigrationTableExists(ctx, db, tableName)
}

// AppliedMigrations gets the already applied migrations from the database, ordered by application date.
func (b NumberedParamsDialect) AppliedMigrations(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	groupName string) ([]string, error) {

	params := make([]any, 0, len(b.AppliedMigrationsParamsOrder))

	for _, p := range b.AppliedMigrationsParamsOrder {
		switch p {
		case ParamNameMGroup:
			params = append(params, groupName)
		default:
			return nil, fmt.Errorf("unexpected param name %v: %w", p, ErrParamNameInvalid)
		}
	}

	rows, rowsErr := db.QueryContext(ctx, fmt.Sprintf(b.AppliedTemplate, tableName), params...)

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
func (b NumberedParamsDialect) RegisterMigration(
	ctx context.Context,
	tx *sql.Tx,
	id string,
	tableName string,
	groupName string) error {

	params := make([]any, 0, len(b.RegisterMigrationParamsOrder))

	for _, p := range b.RegisterMigrationParamsOrder {
		switch p {
		case ParamNameID:
			params = append(params, id)
		case ParamNameMGroup:
			params = append(params, groupName)
		default:
			return fmt.Errorf("unexpected param name %v: %w", p, ErrParamNameInvalid)
		}
	}

	_, err := tx.ExecContext(ctx, fmt.Sprintf(b.RegisterTemplate, tableName), params...)

	return wrapIfError("could not register migration", err)
}
