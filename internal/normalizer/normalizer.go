package normalizer

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/portcullis-co/portcullis-api/internal/types"
	"google.golang.org/api/iterator"
)

func NormalizeRows(warehouseType types.WarehouseType, rows interface{}, schema interface{}) (types.NormalizedData, error) {
	switch warehouseType {
	case types.WarehouseTypeClickhouse, types.WarehouseTypePostgres, types.WarehouseTypeSnowflake, types.WarehouseTypeRedshift:
		sqlRows, ok := rows.(*sql.Rows)
		if !ok {
			return types.NormalizedData{}, fmt.Errorf("invalid row type for SQL-based warehouse")
		}
		return normalizeSQLRows(sqlRows)
	case types.WarehouseTypeBigQuery:
		bigQueryIter, ok := rows.(*bigquery.RowIterator)
		if !ok {
			return types.NormalizedData{}, fmt.Errorf("invalid row type for BigQuery")
		}
		return normalizeBigQueryRows(bigQueryIter)
	default:
		return types.NormalizedData{}, fmt.Errorf("unsupported warehouse type: %s", warehouseType)
	}
}

func normalizeSQLRows(rows *sql.Rows) (types.NormalizedData, error) {
	var normalizedData types.NormalizedData

	// Get column names and types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return types.NormalizedData{}, err
	}

	for _, col := range columnTypes {
		normalizedData.Columns = append(normalizedData.Columns, col.Name())
	}

	// Iterate over rows
	for rows.Next() {
		values := make([]interface{}, len(columnTypes))
		valuePtrs := make([]interface{}, len(columnTypes))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return types.NormalizedData{}, err
		}

		normalizedRow := make([]interface{}, len(columnTypes))
		for i, v := range values {
			normalizedRow[i] = normalizeValue(v, columnTypes[i].DatabaseTypeName())
		}
		normalizedData.Rows = append(normalizedData.Rows, normalizedRow)
	}

	return normalizedData, nil
}

func normalizeBigQueryRows(iter *bigquery.RowIterator) (types.NormalizedData, error) {
	var normalizedData types.NormalizedData

	// Get the schema
	schema := iter.Schema
	for _, field := range schema {
		normalizedData.Columns = append(normalizedData.Columns, field.Name)
	}

	// Iterate over rows
	for {
		var row []bigquery.Value
		err := iter.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return types.NormalizedData{}, err
		}

		normalizedRow := make([]interface{}, len(row))
		for i, v := range row {
			normalizedRow[i] = normalizeBigQueryValue(v)
		}
		normalizedData.Rows = append(normalizedData.Rows, normalizedRow)
	}

	return normalizedData, nil
}

func normalizeValue(val interface{}, dbType string) types.NormalizedValue {
	switch v := val.(type) {
	case nil:
		return types.NormalizedValue{Type: "null", Value: nil}
	case int, int8, int16, int32, int64:
		return types.NormalizedValue{Type: "integer", Value: v}
	case float32, float64:
		return types.NormalizedValue{Type: "float", Value: v}
	case bool:
		return types.NormalizedValue{Type: "boolean", Value: v}
	case string:
		return types.NormalizedValue{Type: "string", Value: v}
	case []byte:
		return types.NormalizedValue{Type: "binary", Value: string(v)}
	case time.Time:
		return types.NormalizedValue{Type: "timestamp", Value: v.Format(time.RFC3339)}
	default:
		// For any other types, convert to string representation
		return types.NormalizedValue{Type: "string", Value: fmt.Sprintf("%v", v)}
	}
}

func normalizeBigQueryValue(val bigquery.Value) types.NormalizedValue {
	switch v := val.(type) {
	case nil:
		return types.NormalizedValue{Type: "null", Value: nil}
	case int64:
		return types.NormalizedValue{Type: "integer", Value: v}
	case float64:
		return types.NormalizedValue{Type: "float", Value: v}
	case bool:
		return types.NormalizedValue{Type: "boolean", Value: v}
	case string:
		return types.NormalizedValue{Type: "string", Value: v}
	case []byte:
		return types.NormalizedValue{Type: "binary", Value: string(v)}
	case time.Time:
		return types.NormalizedValue{Type: "timestamp", Value: v.Format(time.RFC3339)}
	case *big.Rat:
		return types.NormalizedValue{Type: "decimal", Value: v.String()}
	case civil.Date:
		return types.NormalizedValue{Type: "date", Value: v.String()}
	case civil.Time:
		return types.NormalizedValue{Type: "time", Value: v.String()}
	case civil.DateTime:
		return types.NormalizedValue{Type: "datetime", Value: v.String()}
	default:
		// For any other types, convert to string representation
		return types.NormalizedValue{Type: "string", Value: fmt.Sprintf("%v", v)}
	}
}
