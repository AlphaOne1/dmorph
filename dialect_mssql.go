// Copyright the dmorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

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
