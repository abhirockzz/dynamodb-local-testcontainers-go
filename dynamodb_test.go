package dynamodblocal

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const (
	tableName    = "demo_table"
	pkColumnName = "demo_pk"
)

func TestIntegration(t *testing.T) {
	ctx := context.Background()

	container, err := RunContainer(ctx)
	require.NoError(t, err)

	// Clean up the container after the test is complete
	t.Cleanup(func() {
		err := container.Terminate(context.Background())
		if err != nil {
			t.Fatalf("container termination failed: %s", err)
		}
	})

	client, err := container.GetDynamoDBClient(context.Background())
	require.NoError(t, err, "failed to get dynamodb client handle")

	err = createTable(client)
	require.NoError(t, err, "dynamodb create table operation failed")

	result, err := client.ListTables(context.Background(), nil)
	require.NoError(t, err, "dynamodb list tables operation failed")

	actualTableName := result.TableNames[0]
	require.Equal(t, tableName, actualTableName)

	value := "test_value"
	err = addDataToTable(client, value)
	require.NoError(t, err, "data should be added to dynamodb table")

	queryResult, err := queryItem(client, value)
	require.NoError(t, err, "data should be queried from dynamodb table")
	//log.Println("queryResult", queryResult)
	require.Equal(t, value, queryResult)

}

func TestIntegrationWithoutEndpointResolver(t *testing.T) {
	ctx := context.Background()

	container, err := RunContainer(ctx)
	require.NoError(t, err, "container should start successfully")

	// clean up the container after the test completion
	t.Cleanup(func() {
		err := container.Terminate(context.Background())
		if err != nil {
			t.Fatalf("container termination failed: %s", err)
		}
	})

	client := dynamodb.New(dynamodb.Options{})

	err = createTable(client)
	require.Error(t, err, "dynamodb table creation should have failed with error")
}

func TestIntegrationWithSharedDB(t *testing.T) {
	ctx := context.Background()

	container, err := RunContainer(ctx, WithSharedDB())
	require.NoError(t, err)

	// Clean up the container after the test is complete
	t.Cleanup(func() {
		err := container.Terminate(context.Background())
		if err != nil {
			t.Fatalf("container termination failed: %s", err)
		}
	})

	client, err := container.GetDynamoDBClient(context.Background())
	require.NoError(t, err, "failed to get dynamodb client handle")

	err = createTable(client)
	require.NoError(t, err, "dynamodb create table operation failed")

	result, err := client.ListTables(context.Background(), nil)
	require.NoError(t, err, "dynamodb list tables operation failed")

	actualTableName := result.TableNames[0]
	require.Equal(t, tableName, actualTableName)

	//stop
	err = container.Stop(context.Background(), aws.Duration(5*time.Second))
	require.NoError(t, err)

	//re-start
	err = container.Start(context.Background())
	require.NoError(t, err)

	//fetch client handle again
	client, err = container.GetDynamoDBClient(context.Background())
	require.NoError(t, err, "failed to get dynamodb client handle")

	//list tables and verify

	result, err = client.ListTables(context.Background(), nil)
	require.NoError(t, err, "dynamodb list tables operation failed")

	actualTableName = result.TableNames[0]
	require.Equal(t, tableName, actualTableName)

	//add and query data
	value := "test_value"
	err = addDataToTable(client, value)
	require.NoError(t, err, "data should be added to dynamodb table")

	queryResult, err := queryItem(client, value)
	require.NoError(t, err, "data should be queried from dynamodb table")
	require.Equal(t, value, queryResult)

}

func TestIntegrationWithoutSharedDB(t *testing.T) {
	ctx := context.Background()

	container, err := RunContainer(ctx)
	require.NoError(t, err)

	// Clean up the container after the test is complete
	t.Cleanup(func() {
		err := container.Terminate(context.Background())
		if err != nil {
			t.Fatalf("container termination failed: %s", err)
		}
	})

	client, err := container.GetDynamoDBClient(context.Background())
	require.NoError(t, err, "failed to get dynamodb client handle")

	err = createTable(client)
	require.NoError(t, err, "dynamodb create table operation failed")

	result, err := client.ListTables(context.Background(), nil)
	require.NoError(t, err, "dynamodb list tables operation failed")

	actualTableName := result.TableNames[0]
	require.Equal(t, tableName, actualTableName)

	//stop
	err = container.Stop(context.Background(), aws.Duration(5*time.Second))
	require.NoError(t, err)

	//re-start
	err = container.Start(context.Background())
	require.NoError(t, err)

	//fetch client handle again
	client, err = container.GetDynamoDBClient(context.Background())
	require.NoError(t, err, "failed to get dynamodb client handle")

	//list tables and verify

	result, err = client.ListTables(context.Background(), nil)
	require.NoError(t, err, "dynamodb list tables operation failed")
	require.Empty(t, result.TableNames, "table should not exist after restarting container")
}

func TestContainerShouldStartWithTelemetryDisabled(t *testing.T) {
	ctx := context.Background()

	container, err := RunContainer(ctx, WithTelemetryDisabled())
	require.NoError(t, err)

	// Clean up the container after the test is complete
	t.Cleanup(func() {
		err := container.Terminate(context.Background())
		if err != nil {
			t.Fatalf("container termination failed: %s", err)
		}
	})
}

func TestContainerShouldStartWithSharedDBEnabledAndTelemetryDisabled(t *testing.T) {
	ctx := context.Background()

	container, err := RunContainer(ctx, WithSharedDB(), WithTelemetryDisabled())
	require.NoError(t, err)

	// Clean up the container after the test is complete
	t.Cleanup(func() {
		err := container.Terminate(context.Background())
		if err != nil {
			t.Fatalf("container termination failed: %s", err)
		}
	})
}

func createTable(client *dynamodb.Client) error {
	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(pkColumnName),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(pkColumnName),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		return err
	}

	//log.Println("created table")
	return nil
}

func addDataToTable(client *dynamodb.Client, val string) error {

	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			pkColumnName: &types.AttributeValueMemberS{Value: val},
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func queryItem(client *dynamodb.Client, val string) (string, error) {

	output, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			pkColumnName: &types.AttributeValueMemberS{Value: val},
		},
	})

	if err != nil {
		return "", err
	}

	result := output.Item[pkColumnName].(*types.AttributeValueMemberS)

	return result.Value, nil
}
