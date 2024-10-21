package datasource

import (
	"database/sql"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
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

	iter, err := client.Query(req.Query).Read(req.Context)
	if err != nil {
		return types.NormalizedData{}, err
	}

	// You'll need to implement a function to normalize BigQuery results
	return normalizer.NormalizeRows(types.WarehouseTypeBigQuery, iter, nil)
}

func Load(req types.LoadRequest, data types.NormalizedData) error {
	switch req.Type {
	case types.BigQueryWarehouse:
		return loadToBigQuery(req, data)
	default:
		return loadToSQL(req, data)
	}
}

func loadToSQL(req types.LoadRequest, data types.NormalizedData) error {
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
		return fmt.Errorf("unsupported data destination type: %s", req.Type)
	}

	if err != nil {
		return err
	}
	defer db.Close()

	// Construct the INSERT query based on the data
	columns := strings.Join(data.Columns, ", ")
	placeholders := strings.Repeat("?, ", len(data.Columns))
	placeholders = strings.TrimSuffix(placeholders, ", ")

	for _, row := range data.Rows {
		values := make([]interface{}, len(data.Columns))
		for i, col := range data.Columns {
			values[i] = row[col]
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", req.Table, columns, placeholders)
		_, err := db.Exec(query, values...)
		if err != nil {
			return err
		}
	}

	return nil
}

func loadToBigQuery(req types.LoadRequest, data types.NormalizedData) error {
	client, err := connectBigQuery(req)
	if err != nil {
		return err
	}
	defer client.Close()

	// Prepare the BigQuery insert request
	table := client.Dataset(req.Dataset).Table(req.Table)
	inserter := table.Inserter()

	// Convert data rows to BigQuery rows
	var rows []*bigquery.Row
	for _, row := range data.Rows {
		bqRow := make(map[string]bigquery.Value)
		for _, col := range data.Columns {
			bqRow[col] = row[col]
		}
		rows = append(rows, &bigquery.Row{Data: bqRow})
	}

	// Insert the rows into BigQuery
	if err := inserter.Put(req.Context, rows); err != nil {
		return err
	}

	return nil
}
