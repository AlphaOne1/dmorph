// SPDX-FileCopyrightText: 2025 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectDB2 returns a Dialect configured for DB2 databases.
func DialectDB2() BaseDialect {
	return BaseDialect{
		CreateTemplate: `
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 
                    FROM SYSIBM.SYSTABLES 
                    WHERE NAME = '%s' AND TYPE = 'T'
                )
                THEN
                    CREATE TABLE "%s" (
                        id        VARCHAR(255) PRIMARY KEY,
                        create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                END IF;
            END`,
		AppliedTemplate: `
            SELECT id
            FROM   "%s"
            ORDER BY create_ts ASC`,
		RegisterTemplate: `
            INSERT INTO "%s" (id)
            VALUES (:id)`,
	}
}
