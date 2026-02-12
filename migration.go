// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

// Package dmorph provides a simple database migration framework.
package dmorph

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"time"
)

// MigrationTableName is the default name for the migration management table in the database.
const MigrationTableName = "migrations"

// ValidTableNameRex is the regular expression used to check if a given migration table name is valid.
var ValidTableNameRex = regexp.MustCompile("^[a-zA-Z0-9_]+$")

// ErrMigrationsUnrelated signals that the set of migrations to apply and the already applied set do not have the
// same (order of) applied migrations. Applying unrelated migrations could severely harm the database.
var ErrMigrationsUnrelated = errors.New("migrations unrelated")

// ErrMigrationsUnsorted indicates that the already applied migrations were not registered in the order
// (using the timestamp) that they should have been registered (using their id).
var ErrMigrationsUnsorted = errors.New("migrations unsorted")

// ErrNoDialect signals that no dialect for the database operations was chosen.
var ErrNoDialect = errors.New("no dialect")

// ErrNoMigrations signals that no migrations were chosen to be applied.
var ErrNoMigrations = errors.New("no migrations")

// ErrNoMigrationTable occurs if there is no migration table present.
var ErrNoMigrationTable = errors.New("no migration table")

// ErrMigrationTableNameInvalid occurs if the migration table does not adhere to ValidTableNameRex.
var ErrMigrationTableNameInvalid = errors.New("invalid migration table name")

// ErrMigrationsTooOld signals that the migrations to be applied are older than the migrations that are already
// present in the database. This error can occur when an older version of the application is started using a database
// used already by a newer version of the application.
var ErrMigrationsTooOld = errors.New("migrations too old")

// Dialect is an interface describing the functionalities needed to manage migrations inside a database.
type Dialect interface {
	EnsureMigrationTableExists(ctx context.Context, db *sql.DB, tableName string) error
	AppliedMigrations(ctx context.Context, db *sql.DB, tableName string) ([]string, error)
	RegisterMigration(ctx context.Context, tx *sql.Tx, id string, tableName string) error
}

// Migration is an interface to provide abstract information about the migration at hand.
type Migration interface {
	Key() string                                   // identifier, used for ordering
	Migrate(ctx context.Context, tx *sql.Tx) error // migration functionality
}

// migrationOrder is used to order Migration instances.
func migrationOrder(m, n Migration) int {
	switch {
	case m.Key() < n.Key():
		return -1
	case m.Key() > n.Key():
		return 1
	default:
		return 0
	}
}

// Morpher contains all the required information to run a given set of database migrations on a database.
type Morpher struct {
	Dialect    Dialect      // database vendor specific dialect
	Migrations []Migration  // migrations to be applied
	TableName  string       // table name for migration management
	Log        *slog.Logger // logger to be used
}

// MorphOption is the type used for functional options.
type MorphOption func(*Morpher) error

// WithDialect sets the vendor-specific database dialect to be used.
func WithDialect(dialect Dialect) MorphOption {
	return func(m *Morpher) error {
		m.Dialect = dialect

		return nil
	}
}

// WithMigrations adds the given migrations to be executed.
func WithMigrations(migrations ...Migration) MorphOption {
	return func(m *Morpher) error {
		m.Migrations = append(m.Migrations, migrations...)

		return nil
	}
}

// WithLog sets the logger that is to be used. If none is supplied, the default logger
// is used instead.
func WithLog(log *slog.Logger) MorphOption {
	return func(m *Morpher) error {
		m.Log = log

		return nil
	}
}

// WithTableName sets the migration table name to the given one. If not supplied, the
// default MigrationTableName is used instead.
func WithTableName(tableName string) func(*Morpher) error {
	return func(m *Morpher) error {
		if len(tableName) < 1 {
			return ErrMigrationTableNameInvalid
		}

		if !ValidTableNameRex.MatchString(tableName) {
			return ErrMigrationTableNameInvalid
		}

		m.TableName = tableName

		return nil
	}
}

// NewMorpher creates a new Morpher configuring it with the given options.
// It ensures that the newly created Morpher has migrations and a database dialect configured.
// If no migration table name is given, the default MigrationTableName is used instead.
func NewMorpher(options ...MorphOption) (*Morpher, error) {
	morpher := &Morpher{
		TableName: MigrationTableName,
		Log:       slog.Default(),
	}

	for _, option := range options {
		if err := option(morpher); err != nil {
			return nil, err
		}
	}

	if validErr := morpher.IsValid(); validErr != nil {
		return nil, validErr
	}

	return morpher, nil
}

// IsValid checks if the Morpher contains all the required information to run.
func (m *Morpher) IsValid() error {
	if m.Dialect == nil {
		return ErrNoDialect
	}

	if len(m.Migrations) < 1 {
		return ErrNoMigrations
	}

	if len(m.TableName) < 1 {
		return ErrNoMigrationTable
	}

	if !ValidTableNameRex.MatchString(m.TableName) {
		return ErrMigrationTableNameInvalid
	}

	return nil
}

// Run runs the configured Morpher on the given database. If the migrations already applied
// to the database are a superset of the migrations the Morpher would apply, ErrMigrationsTooOld is
// returned.
// Run will run each migration in a separate transaction, with the last step to register the
// migration in the migration table.
func (m *Morpher) Run(ctx context.Context, db *sql.DB) error {
	if validErr := m.IsValid(); validErr != nil {
		return validErr
	}

	if err := m.Dialect.EnsureMigrationTableExists(ctx, db, m.TableName); err != nil {
		return fmt.Errorf("could not create migration table: %w", err)
	}

	appliedMigrations, appliedMigrationsErr := m.Dialect.AppliedMigrations(ctx, db, m.TableName)

	if appliedMigrationsErr != nil {
		return fmt.Errorf("could not get applied migrations: %w", appliedMigrationsErr)
	}

	slices.SortFunc(m.Migrations, migrationOrder)
	lastMigration := ""

	if len(appliedMigrations) == 0 {
		m.Log.Debug("no previous migrations")
	} else {
		m.Log.Debug("last migration",
			slog.String("file", appliedMigrations[len(appliedMigrations)-1]))

		err := m.checkAppliedMigrations(appliedMigrations)
		if err != nil {
			return err
		}

		lastMigration = appliedMigrations[len(appliedMigrations)-1]
	}

	return m.applyMigrations(ctx, db, lastMigration)
}

// applyMigrations applies the given migrations to the database.
// This method does not check for the validity or consistency of the database.
func (m *Morpher) applyMigrations(ctx context.Context, db *sql.DB, lastMigration string) error {
	var startMigration time.Time

	for _, migration := range m.Migrations {
		if lastMigration >= migration.Key() {
			m.Log.Info("migration already applied", slog.String("file", migration.Key()))

			continue
		}

		m.Log.Info("applying migration", slog.String("file", migration.Key()))
		startMigration = time.Now()

		// Check context before starting a transaction
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled before migration %s: %w", migration.Key(), err)
		}

		if err := m.runOneMigration(ctx, db, migration); err != nil {
			return err
		}

		m.Log.Info("migration applied",
			slog.String("file", migration.Key()),
			slog.Duration("duration", time.Since(startMigration)),
		)
	}

	return nil
}

// runOneMigration executes a single migration within a database transaction and logs its completion.
func (m *Morpher) runOneMigration(ctx context.Context, db *sql.DB, mig Migration) error {
	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	// Even if we are sure to catch all possibilities, we use this as a safeguard that also with later
	// modifications. When a successful commit cannot be done, at least the rollback is executed, freeing
	// allocated resources of the transaction.
	defer func() { _ = tx.Rollback() }()

	if err := mig.Migrate(ctx, tx); err != nil {
		rollbackErr := tx.Rollback()

		return errors.Join(err, rollbackErr)
	}

	if err := m.Dialect.RegisterMigration(ctx, tx, mig.Key(), m.TableName); err != nil {
		rollbackErr := tx.Rollback()

		return errors.Join(err, rollbackErr)
	}

	if commitErr := tx.Commit(); commitErr != nil {
		rollbackErr := tx.Rollback()

		return errors.Join(commitErr, rollbackErr)
	}

	return nil
}

// checkAppliedMigrations checks if the already applied migrations in the database are consistent.
// This means inherently in them and also regarding the migrations that are to be applied.
func (m *Morpher) checkAppliedMigrations(appliedMigrations []string) error {
	if !slices.IsSorted(appliedMigrations) {
		m.Log.Error("migrations not applied in order")

		return ErrMigrationsUnsorted
	}

	if m.Migrations[len(m.Migrations)-1].Key() < appliedMigrations[len(appliedMigrations)-1] {
		return ErrMigrationsTooOld
	}

	if len(m.Migrations) < len(appliedMigrations) {
		// it is impossible to have a migration newer than the one already applied
		// without having at least the same number of previous migrations
		return ErrMigrationsUnrelated
	}

	// we know here that there are at least as many migrations applied as we got to apply
	for i := range appliedMigrations {
		if appliedMigrations[i] != m.Migrations[i].Key() {
			return ErrMigrationsUnrelated
		}
	}

	return nil
}

// Run is a convenience function to easily get the migration job done. For more control use the
// Morpher directly.
func Run(ctx context.Context, db *sql.DB, options ...MorphOption) error {
	m, morphErr := NewMorpher(options...)

	if morphErr != nil {
		return morphErr
	}

	return m.Run(ctx, db)
}
