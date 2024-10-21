package datasource

import (
	"database/sql"
	"fmt"

	"github.com/portcullis-co/portcullis-api/internal/types"
)

func connectSnowflake(req types.ExtractRequest) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@%s:%d/%s",
		req.Username, req.Password, req.Host, req.Port, req.Database)
	return sql.Open("snowflake", dsn)
}
