// Copyright the dmorph contributors.
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
)

type FileMigration struct {
	Name          string
	FS            fs.FS
	migrationFunc func(tx *sql.Tx, migration string) error
}

func (f FileMigration) Key() string {
	return f.Name
}

func (f FileMigration) Migrate(tx *sql.Tx) error {
	return f.migrationFunc(tx, f.Name)
}

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

				return applyFileSteps(tx, m, migration, morpher.Log)
			},
		})

		return nil
	}
}

func WithMigrationFromFileFS(name string, dir fs.FS) MorphOption {
	return func(morpher *Morpher) error {
		morpher.Migrations = append(morpher.Migrations, migrationFromFileFS(name, dir, morpher.Log))

		return nil
	}
}

func WithMigrationsFromFS(d fs.ReadDirFS) MorphOption {
	return func(morpher *Morpher) error {
		dirEntry, err := d.ReadDir(".")

		if err != nil {
			return err
		}

		for _, entry := range dirEntry {
			morpher.Log.Info("entry", slog.String("name", entry.Name()))
			if entry.Type().IsRegular() {
				morpher.Migrations = append(morpher.Migrations,
					migrationFromFileFS(entry.Name(), d, morpher.Log))
			}
		}

		return nil
	}
}

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

			return applyFileSteps(tx, m, migration, log)
		},
	}
}

func applyFileSteps(tx *sql.Tx, r io.Reader, id string, log *slog.Logger) error {
	buf := bytes.Buffer{}

	scanner := bufio.NewScanner(r)

	for i := 0; scanner.Scan(); {
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

	return scanner.Err()
}
