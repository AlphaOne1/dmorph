// SPDX-FileCopyrightText: 2025 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// FileMigration implements the Migration interface. It helps to apply migrations from a file or fs.FS.
type FileMigration struct {
	Name          string
	FS            fs.FS
	migrationFunc func(ctx context.Context, tx *sql.Tx, migration string) error
}

// Key returns the key of the migration to register in the migration table.
func (f FileMigration) Key() string {
	return f.Name
}

// Migrate executes the migration on the given transaction.
func (f FileMigration) Migrate(ctx context.Context, tx *sql.Tx) error {
	return f.migrationFunc(ctx, tx, f.Name)
}

// WithMigrationFromFile generates a FileMigration that will run the content of the given file.
func WithMigrationFromFile(name string) MorphOption {
	return func(morpher *Morpher) error {
		morpher.Migrations = append(morpher.Migrations, FileMigration{
			Name: name,
			migrationFunc: func(ctx context.Context, tx *sql.Tx, migration string) error {
				m, mErr := os.Open(filepath.Clean(migration))

				if mErr != nil {
					return wrapIfError("could not open file "+migration, mErr)
				}

				defer func() { _ = m.Close() }()

				return applyStepsStream(ctx, tx, m, migration, morpher.Log)
			},
		})

		return nil
	}
}

// WithMigrationFromFileFS generates a FileMigration that will run the content of the given file from the
// given filesystem.
func WithMigrationFromFileFS(name string, dir fs.FS) MorphOption {
	return func(morpher *Morpher) error {
		morpher.Migrations = append(morpher.Migrations, migrationFromFileFS(name, dir, morpher.Log))

		return nil
	}
}

// WithMigrationsFromFS generates a FileMigration that will run all migration scripts of the files in the given
// filesystem.
func WithMigrationsFromFS(d fs.FS) MorphOption {
	return func(morpher *Morpher) error {
		dirEntry, err := fs.ReadDir(d, ".")

		if err == nil {
			for _, entry := range dirEntry {
				morpher.Log.Info("entry", slog.String("name", entry.Name()))

				if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ".sql") {
					morpher.Migrations = append(morpher.Migrations,
						migrationFromFileFS(entry.Name(), d, morpher.Log))
				}
			}
		}

		return wrapIfError("could not read directory", err)
	}
}

// migrationFromFileFS creates a FileMigration instance for a specific migration file from a fs.FS directory.
func migrationFromFileFS(name string, dir fs.FS, log *slog.Logger) FileMigration {
	return FileMigration{
		Name: name,
		FS:   dir,
		migrationFunc: func(ctx context.Context, tx *sql.Tx, migration string) error {
			m, mErr := dir.Open(migration)

			if mErr != nil {
				return wrapIfError("could not open file migration", mErr)
			}

			defer func() { _ = m.Close() }()

			return applyStepsStream(ctx, tx, m, migration, log)
		},
	}
}

// applyStepsStream executes database migration steps read from an io.Reader, separated by semicolons, in a transaction.
// Returns the corresponding error if any step execution fails. Also, as some database drivers or engines seem to not
// support comments, leading comments are removed. This function does not undertake efforts to scan the SQL to find
// other comments. Such leading comments telling what a step is going to do, work. But comments in the middle of a
// statement will not be removed. At least with SQLite this will lead to hard-to-find errors.
func applyStepsStream(ctx context.Context, tx *sql.Tx, r io.Reader, migrationID string, log *slog.Logger) error {
	const InitialScannerBufSize = 64 * 1024
	const MaxScannerBufSize = 1024 * 1024

	buf := bytes.Buffer{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, InitialScannerBufSize), MaxScannerBufSize)
	newStep := true
	var step int

	for step = 0; scanner.Scan(); {
		if newStep && strings.HasPrefix(scanner.Text(), "--") {
			// skip leading comments
			continue
		}

		if scanner.Text() == ";" {
			log.Info("migration step",
				slog.String("migrationID", migrationID),
				slog.Int("step", step),
			)

			if _, err := tx.ExecContext(ctx, buf.String()); err != nil {
				return fmt.Errorf("apply migration %q step %d: %w", migrationID, step, err)
			}

			buf.Reset()
			newStep = true
			step++

			continue
		}

		// Append the current line (preserve formatting by adding a newline between lines)
		if buf.Len() > 0 {
			buf.WriteByte('\n')
		}

		buf.Write(scanner.Bytes())
		newStep = false
	}

	// cleanup after, for the final statement without the closing `;` on a new line
	if buf.Len() > 0 {
		log.Info("migration step",
			slog.String("migrationID", migrationID),
			slog.Int("step", step),
		)

		if _, err := tx.ExecContext(ctx, buf.String()); err != nil {
			return fmt.Errorf("apply migration %q step %d (final): %w", migrationID, step, err)
		}
	}

	return wrapIfError("scanner error", scanner.Err())
}
