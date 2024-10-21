package datasource

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/portcullis-co/portcullis-api/internal/types"
)

func connectRedshift(req types.ExtractRequest) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=require",
		req.Host, req.Port, req.Database, req.Username, req.Password)
	return sql.Open("postgres", dsn)
}
