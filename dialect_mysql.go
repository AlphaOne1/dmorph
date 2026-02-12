// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectMySQL returns a Dialect configured for MySQL databases.
func DialectMySQL() BaseDialect {
	return BaseDialect{
		CreateTemplate: "CREATE TABLE IF NOT EXISTS `%s`" + ` (
				id        VARCHAR(255) PRIMARY KEY,
				create_ts TIMESTAMP DEFAULT current_timestamp
			)`,
		AppliedTemplate:  "SELECT id FROM `%s` ORDER BY create_ts ASC",
		RegisterTemplate: "INSERT INTO `%s` (id) VALUES(:id)",
	}
}
