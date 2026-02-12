// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectCSVQ returns a Dialect configured for CSVQ databases.
func DialectCSVQ() BaseDialect {
	return BaseDialect{
		CreateTemplate: `
			CREATE TABLE IF NOT EXISTS %s (
				id,
				create_ts
			)`,
		AppliedTemplate: `
			SELECT id
			FROM   %s
	        ORDER BY create_ts ASC`,
		RegisterTemplate: `
			INSERT INTO %s (id)
	        VALUES(:id)`,
	}
}
