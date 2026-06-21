// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectMSSQL returns a Dialect configured for Microsoft SQL Server databases.
func DialectMSSQL() NamedParamsDialect {
	return NamedParamsDialect{
		CreateTemplate: `
            IF NOT EXISTS (
                SELECT *
                FROM sys.tables
                WHERE name = '%s'
            )
            CREATE TABLE [%s] (
                id        NVARCHAR(255) NOT NULL,
                mgroup    NVARCHAR(255) NOT NULL,
                create_ts DATETIME DEFAULT GETDATE(),
                PRIMARY KEY (id, mgroup)
            )`,
		AppliedTemplate: `
            SELECT id
            FROM   [%s]
            WHERE  mgroup = @mgroup
            ORDER BY create_ts ASC`,
		RegisterTemplate: `
            INSERT INTO [%s] (id, mgroup)
            VALUES (@id, @mgroup)`,
	}
}
