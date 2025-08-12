// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"bufio"
	"bytes"
	"database/sql"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"strings"
)

// FileMigration implements the Migration interface. It helps to apply migrations from a file or fs.FS.
type FileMigration struct {
	Name          string
	FS            fs.FS
	migrationFunc func(tx *sql.Tx, migration string) error
}

// Key returns the key of the migration to register in the migration table.
func (f FileMigration) Key() string {
	return f.Name
}

// Migrate executes the migration on the given transaction.
func (f FileMigration) Migrate(tx *sql.Tx) error {
	return f.migrationFunc(tx, f.Name)
}

// WithMigrationFromFile generates a FileMigration that will run the content of the given file.
func WithMigrationFromFile(name string) MorphOption {
	return func(morpher *Morpher) error {
		morpher.Migrations = append(morpher.Migrations, FileMigration{
			Name: name,
			migrationFunc: func(tx *sql.Tx, migration string) error {
				m, mErr := os.Open(migration)

				if mErr != nil {
					return mErr
				}

				defer func() { _ = m.Close() }()

				return applyStepsStream(tx, m, migration, morpher.Log)
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
func WithMigrationsFromFS(d fs.ReadDirFS) MorphOption {
	return func(morpher *Morpher) error {
		dirEntry, err := d.ReadDir(".")

		if err == nil {
			for _, entry := range dirEntry {
				morpher.Log.Info("entry", slog.String("name", entry.Name()))

				if entry.Type().IsRegular() {
					morpher.Migrations = append(morpher.Migrations,
						migrationFromFileFS(entry.Name(), d, morpher.Log))
				}
			}
		}

		return err
	}
}

// migrationFromFileFS creates a FileMigration instance for a specific migration file from a fs.FS directory.
func migrationFromFileFS(name string, dir fs.FS, log *slog.Logger) FileMigration {
	return FileMigration{
		Name: name,
		FS:   dir,
		migrationFunc: func(tx *sql.Tx, migration string) error {
			m, mErr := dir.Open(migration)

			if mErr != nil {
				return mErr
			}

			defer func() { _ = m.Close() }()

			return applyStepsStream(tx, m, migration, log)
		},
	}
}

// applyStepsStream executes database migration steps read from an io.Reader, separated by semicolons, in a transaction.
// Returns the corresponding error if any step execution fails. Also, as some database drivers or engines seem to not
// support comments, leading comments are removed. This function does not undertake efforts to scan the SQL to find
// other comments. Such leading comments telling what a step is going to do, work. But comments in the middle of a
// statement will not be removed. At least with SQLite this will lead to hard-to-find errors.
func applyStepsStream(tx *sql.Tx, r io.Reader, id string, log *slog.Logger) error {
	const InitialScannerBufSize = 64 * 1024
	const MaxScannerBufSize = 1024 * 1024

	buf := bytes.Buffer{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, InitialScannerBufSize), MaxScannerBufSize)
	newStep := true
	var i int

	for i = 0; scanner.Scan(); {
		if newStep && strings.HasPrefix(scanner.Text(), "--") {
			// skip leading comments
			continue
		}

		newStep = false

		buf.Write(scanner.Bytes())

		if scanner.Text() == ";" {
			log.Info("migration step",
				slog.String("id", id),
				slog.Int("step", i),
			)
			if _, err := tx.Exec(buf.String()); err != nil {
				return err
			}

			buf.Reset()
			i++
		}
	}

	// cleanup after, for the final statement without the closing `;` on a new line
	if buf.Len() > 0 {
		log.Info("migration step",
			slog.String("id", id),
			slog.Int("step", i),
		)

		if _, err := tx.Exec(buf.String()); err != nil {
			return err
		}
	}

	return scanner.Err()
}
