package datasource

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/portcullis-co/portcullis-api/internal/types"
	"google.golang.org/api/option"
)

// BigQueryClient wraps the BigQuery client to allow for custom query execution
type BigQueryClient struct {
	*bigquery.Client
	ctx context.Context
}

func connectBigQuery(req types.ExtractRequest) (*BigQueryClient, error) {
	ctx := context.Background()

	// Create a BigQuery client
	client, err := bigquery.NewClient(ctx, req.Database, option.WithCredentialsJSON([]byte(req.Password)))
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	// Return our custom BigQueryClient
	return &BigQueryClient{
		Client: client,
		ctx:    ctx,
	}, nil
}

// Query executes a BigQuery query and returns the results
func (c *BigQueryClient) Query(query string) (*bigquery.RowIterator, error) {
	q := c.Client.Query(query)
	return q.Read(c.ctx)
}

// Close closes the BigQuery client
func (c *BigQueryClient) Close() error {
	return c.Client.Close()
}
