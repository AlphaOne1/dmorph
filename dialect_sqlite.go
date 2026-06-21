// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectSQLite returns a Dialect configured for SQLite databases.
//
//nolint:goconst
func DialectSQLite() NamedParamsDialect {
	return NamedParamsDialect{
		CreateTemplate: `
			CREATE TABLE IF NOT EXISTS "%s" (
				id        VARCHAR(255) NOT NULL,
				mgroup    VARCHAR(255) NOT NULL,
				create_ts TIMESTAMP DEFAULT current_timestamp,
			    PRIMARY KEY (id, mgroup)
			)`,
		AppliedTemplate: `
			SELECT id
			FROM   "%s"
			WHERE  mgroup = :mgroup
	        ORDER BY create_ts ASC`,
		RegisterTemplate: `
			INSERT INTO "%s" (id, mgroup)
	        VALUES(:id, :mgroup)`,
	}
}
