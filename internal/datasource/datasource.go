package datasource

import (
	"database/sql"
	"fmt"

	"github.com/portcullis-co/portcullis-api/internal/normalizer"
	"github.com/portcullis-co/portcullis-api/internal/types"
)

func Extract(req types.ExtractRequest) (types.NormalizedData, error) {
	switch req.Type {
	case types.BigQueryWarehouse:
		return extractFromBigQuery(req)
	default:
		return extractFromSQL(req)
	}
}

func extractFromSQL(req types.ExtractRequest) (types.NormalizedData, error) {
	var db *sql.DB
	var err error

	switch req.Type {
	case types.ClickhouseWarehouse:
		db, err = connectClickhouse(req)
	case types.PostgresWarehouse:
		db, err = connectPostgres(req)
	case types.SnowflakeWarehouse:
		db, err = connectSnowflake(req)
	case types.RedshiftWarehouse:
		db, err = connectRedshift(req)
	default:
		return types.NormalizedData{}, fmt.Errorf("unsupported data source type: %s", req.Type)
	}

	if err != nil {
		return types.NormalizedData{}, err
	}
	defer db.Close()

	rows, err := db.Query(req.Query)
	if err != nil {
		return types.NormalizedData{}, err
	}
	defer rows.Close()

	return normalizer.NormalizeRows(req.Type, rows, nil)
}

func extractFromBigQuery(req types.ExtractRequest) (types.NormalizedData, error) {
	client, err := connectBigQuery(req)
	if err != nil {
		return types.NormalizedData{}, err
	}
	defer client.Close()

	iter, err := client.Query(req.Query)
	if err != nil {
		return types.NormalizedData{}, err
	}

	// You'll need to implement a function to normalize BigQuery results
	return normalizer.NormalizeRows(types.WarehouseTypeBigQuery, iter, nil)
}
