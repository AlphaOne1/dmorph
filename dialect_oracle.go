// Copyright the DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

// DialectOracle returns a Dialect configured for Oracle Database.
func DialectOracle() BaseDialect {
	return BaseDialect{
		CreateTemplate: `
            BEGIN
                EXECUTE IMMEDIATE '
                    CREATE TABLE "%s" (
                        id        VARCHAR2(255) PRIMARY KEY,
                        create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;`,
		AppliedTemplate: `
            SELECT id
            FROM   "%s"
            ORDER BY create_ts ASC`,
		RegisterTemplate: `
            INSERT INTO "%s" (id)
            VALUES (:id)`,
	}
}
