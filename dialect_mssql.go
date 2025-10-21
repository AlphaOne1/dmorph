// SPDX-FileCopyrightText: 2025 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectMSSQL returns a Dialect configured for Microsoft SQL Server databases.
func DialectMSSQL() BaseDialect {
	return BaseDialect{
		CreateTemplate: `
            IF NOT EXISTS (
                SELECT *
                FROM sysobjects
                WHERE name = '%s' AND xtype = 'U'
            )
            CREATE TABLE [%s] (
                id        NVARCHAR(255) PRIMARY KEY,
                create_ts DATETIME DEFAULT GETDATE()
            )`,
		AppliedTemplate: `
            SELECT id
            FROM   [%s]
            ORDER BY create_ts ASC`,
		RegisterTemplate: `
            INSERT INTO [%s] (id)
            VALUES (@id)`,
	}
}
