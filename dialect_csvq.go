// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectCSVQ returns a Dialect configured for CSVQ databases.
func DialectCSVQ() NamedParamsDialect {
	return NamedParamsDialect{
		CreateTemplate: `
			CREATE TABLE IF NOT EXISTS %s (
				id,
				mgroup,
				create_ts
			)`,
		AppliedTemplate: `
			SELECT id
			FROM   %s
			WHERE  mgroup = :mgroup
	        ORDER BY create_ts ASC`,
		RegisterTemplate: `
			INSERT INTO %s (id, mgroup)
	        VALUES(:id, :mgroup)`,
	}
}
