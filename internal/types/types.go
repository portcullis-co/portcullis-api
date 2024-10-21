package types

const (
	ClickhouseWarehouse WarehouseType = "clickhouse"
	PostgresWarehouse   WarehouseType = "postgres"
	SnowflakeWarehouse  WarehouseType = "snowflake"
	RedshiftWarehouse   WarehouseType = "redshift"
	BigQueryWarehouse   WarehouseType = "bigquery"
)

type WarehouseType string

const (
	WarehouseTypeClickhouse WarehouseType = "clickhouse"
	WarehouseTypePostgres   WarehouseType = "postgres"
	WarehouseTypeSnowflake  WarehouseType = "snowflake"
	WarehouseTypeRedshift   WarehouseType = "redshift"
	WarehouseTypeBigQuery   WarehouseType = "bigquery"
)

type ExtractRequest struct {
	Type     WarehouseType `json:"type"`
	Host     string        `json:"host"`
	Port     int           `json:"port"`
	Database string        `json:"database"`
	Username string        `json:"username"`
	Password string        `json:"password"`
	Query    string        `json:"query"`
}

type LoadRequest struct {
	Type     WarehouseType `json:"type"`
	Host     string        `json:"host"`
	Port     int           `json:"port"`
	Database string        `json:"database"`
	Username string        `json:"username"`
	Password string        `json:"password"`
	Query    string        `json:"query"`
}

type TransferRequest struct {
	Source      ExtractRequest `json:"source"`
	Destination LoadRequest    `json:"destination"`
}

type NormalizedData struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

type NormalizedValue struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Function to validate WarehouseType
func IsValidWarehouseType(wt WarehouseType) bool {
	switch wt {
	case WarehouseTypeClickhouse, WarehouseTypePostgres, WarehouseTypeSnowflake, WarehouseTypeRedshift, WarehouseTypeBigQuery:
		return true
	default:
		return false
	}
}
