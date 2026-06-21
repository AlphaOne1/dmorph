// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectDB2 returns a Dialect configured for DB2 databases.
func DialectDB2() NamedParamsDialect {
	return NamedParamsDialect{
		CreateTemplate: `
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM SYSIBM.SYSTABLES
                    WHERE NAME = '%s' AND TYPE = 'T'
                )
                THEN
                    CREATE TABLE "%s" (
                        id        VARCHAR(255) NOT NULL,
                        mgroup    VARCHAR(255) NOT NULL,
                        create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (id, mgroup)
                    );
                END IF;
            END`,
		AppliedTemplate: `
            SELECT id
            FROM   "%s"
            WHERE  mgroup = :mgroup
            ORDER BY create_ts ASC`,
		RegisterTemplate: `
            INSERT INTO "%s" (id, mgroup)
            VALUES (:id, :mgroup)`,
	}
}
