package dynamodblocal

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DynamodbLocalContainer represents the a DynamoDB Local container - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
type DynamodbLocalContainer struct {
	testcontainers.Container
}

const (
	image         = "amazon/dynamodb-local:2.2.1"
	port          = nat.Port("8000/tcp")
	containerName = "dynamodb_local"
)

// RunContainer creates an instance of the dynamodb container type
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*DynamodbLocalContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{string(port)},
		WaitingFor:   wait.ForListeningPort(port),
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	for _, opt := range opts {
		opt.Customize(&genericContainerReq)
	}

	//log.Println("CMD", genericContainerReq.Cmd)

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, err
	}

	return &DynamodbLocalContainer{Container: container}, nil
}

// ConnectionString returns DynamoDB local endpoint host and port in <host>:<port> format
func (c *DynamodbLocalContainer) ConnectionString(ctx context.Context) (string, error) {
	mappedPort, err := c.MappedPort(ctx, port)
	if err != nil {
		return "", err
	}

	hostIP, err := c.Host(ctx)
	if err != nil {
		return "", err
	}

	uri := fmt.Sprintf("%s:%s", hostIP, mappedPort.Port())
	return uri, nil
}

func (c *DynamodbLocalContainer) GetDynamoDBClient(ctx context.Context) (*dynamodb.Client, error) {
	hostAndPort, err := c.ConnectionString(context.Background())
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID:     "DUMMYIDEXAMPLE",
			SecretAccessKey: "DUMMYEXAMPLEKEY",
		},
	}))
	if err != nil {
		return nil, err
	}

	return dynamodb.NewFromConfig(cfg, dynamodb.WithEndpointResolverV2(&DynamoDBLocalResolver{hostAndPort: hostAndPort})), nil
}

// WithSharedDB allows container reuse between successive runs. Data will be persisted
func WithSharedDB() testcontainers.CustomizeRequestOption {

	return func(req *testcontainers.GenericContainerRequest) {
		if len(req.Cmd) > 0 {
			req.Cmd = append(req.Cmd, "-sharedDb")
		} else {
			req.Cmd = append(req.Cmd, "-jar", "DynamoDBLocal.jar", "-sharedDb")
		}
		req.Name = containerName
		req.Reuse = true
	}
}

// WithTelemetryDisabled - DynamoDB local will not send any telemetry
func WithTelemetryDisabled() testcontainers.CustomizeRequestOption {

	return func(req *testcontainers.GenericContainerRequest) {
		// if other flags (e.g. -sharedDb) exist, append to them
		if len(req.Cmd) > 0 {
			req.Cmd = append(req.Cmd, "-disableTelemetry")
		} else {
			req.Cmd = append(req.Cmd, "-jar", "DynamoDBLocal.jar", "-disableTelemetry")
		}
	}
}
