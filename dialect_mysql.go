// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectMySQL returns a Dialect configured for MySQL databases.
func DialectMySQL() NumberedParamsDialect {
	return NumberedParamsDialect{
		NamedParamsDialect: NamedParamsDialect{
			CreateTemplate: "CREATE TABLE IF NOT EXISTS `%s`" + ` (
				id        VARCHAR(255) NOT NULL,
				mgroup    VARCHAR(255) NOT NULL,
				create_ts TIMESTAMP DEFAULT current_timestamp,
				PRIMARY KEY (id, mgroup)
			)`,
			AppliedTemplate:  "SELECT id FROM `%s` WHERE mgroup = ? ORDER BY create_ts ASC",
			RegisterTemplate: "INSERT INTO `%s` (id, mgroup) VALUES(?, ?)",
		},
		AppliedMigrationsParamsOrder: []ParamName{
			ParamNameMGroup,
		},

		RegisterMigrationParamsOrder: []ParamName{
			ParamNameID,
			ParamNameMGroup,
		},
	}
}
