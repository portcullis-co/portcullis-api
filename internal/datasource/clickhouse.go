package datasource

import (
	"database/sql"
	"fmt"

	"github.com/portcullis-co/portcullis-api/internal/types"
)

func connectClickhouse(req types.ExtractRequest) (*sql.DB, error) {
	dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
		req.Host, req.Port, req.Database, req.Username, req.Password)
	return sql.Open("clickhouse", dsn)
}
