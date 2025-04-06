// Copyright the dmorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"database/sql"
	"fmt"
)

type BaseDialect struct {
	CreateTemplate   string
	AppliedTemplate  string
	RegisterTemplate string
}

func (b BaseDialect) EnsureMigrationTableExists(db *sql.DB, tableName string) error {
	_, execErr := db.Exec(fmt.Sprintf(b.CreateTemplate, tableName))

	return execErr
}

func (b BaseDialect) AppliedMigrations(db *sql.DB, tableName string) ([]string, error) {
	rows, rowsErr := db.Query(fmt.Sprintf(b.AppliedTemplate, tableName))

	if rowsErr != nil {
		return nil, rowsErr
	}

	defer func() { _ = rows.Close() }()

	var result []string
	var tmp string

	for rows.Next() {
		if scanErr := rows.Scan(&tmp); scanErr != nil {
			return nil, scanErr
		}
		result = append(result, tmp)
	}

	return result, nil
}

func (b BaseDialect) RegisterMigration(tx *sql.Tx, id string, tableName string) error {
	_, err := tx.Exec(fmt.Sprintf(b.RegisterTemplate, tableName),
		sql.Named("id", id))

	return err
}

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

func DialectPostgres() BaseDialect {
	return BaseDialect{
		CreateTemplate: `
			CREATE TABLE IF NOT EXISTS "%s" (
				id        VARCHAR(255) PRIMARY KEY,
				create_ts TIMESTAMP DEFAULT current_timestamp
			)`,
		AppliedTemplate: `
			SELECT id
			FROM   "%s"
	        ORDER BY create_ts ASC`,
		RegisterTemplate: `
			INSERT INTO "%s" (id)
	        VALUES(:id)`,
	}
}

func DialectSQLite() BaseDialect {
	return BaseDialect{
		CreateTemplate: `
			CREATE TABLE IF NOT EXISTS "%s" (
				id        VARCHAR(255) PRIMARY KEY,
				create_ts TIMESTAMP DEFAULT current_timestamp
			)`,
		AppliedTemplate: `
			SELECT id
			FROM   "%s"
	        ORDER BY create_ts ASC`,
		RegisterTemplate: `
			INSERT INTO "%s" (id)
	        VALUES(:id)`,
	}
}
