package dmorph

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDialectStatements(t *testing.T) {
	// we cannot run tests against all databases, but at least we can test
	// that the statements for the databases are somehow filled
	tests := []struct {
		name   string
		caller func() BaseDialect
	}{
		{name: "DB2", caller: DialectDB2},
		{name: "MSSQL", caller: DialectMSSQL},
		{name: "MySQL", caller: DialectMySQL},
		{name: "Oracle", caller: DialectOracle},
		{name: "Postgres", caller: DialectPostgres},
		{name: "SQLite", caller: DialectSQLite},
	}

	for k, v := range tests {
		d := v.caller()

		if len(d.CreateTemplate) < 10 {
			t.Errorf("%v: create template is too short for %v", k, v.name)
		}
		assert.Contains(t, d.CreateTemplate, "%s",
			"no table name placeholder in create template for", v.name)

		if len(d.AppliedTemplate) < 10 {
			t.Errorf("%v: applied template is too short for %v", k, v.name)
		}
		assert.Contains(t, d.AppliedTemplate, "%s",
			"no table name placeholder in applied template for", v.name)

		if len(d.RegisterTemplate) < 10 {
			t.Errorf("%v: register template is too short for %v", k, v.name)
		}
		assert.Contains(t, d.RegisterTemplate, "%s",
			"no table name placeholder in register template for", v.name)
	}
}
