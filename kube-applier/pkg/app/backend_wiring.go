// Copyright 2026 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/aws/aws-sdk-go-v2/config"

	cosmosbk "github.com/Azure/ARO-HCP/internal/database/backends/cosmos"
	dynamobk "github.com/Azure/ARO-HCP/internal/database/backends/dynamodb"

	"github.com/Azure/ARO-HCP/internal/database"
	"github.com/Azure/ARO-HCP/internal/utils"
)

// BackendType enumerates the supported storage backends.
type BackendType string

const (
	BackendTypeCosmos  BackendType = "cosmos"
	BackendTypeDynamoDB BackendType = "dynamodb"
)

// BackendConfig carries the configuration needed to construct any supported
// StorageBackend.  Only the fields relevant to the chosen Backend are required.
type BackendConfig struct {
	Backend BackendType

	// Cosmos fields — required when Backend == "cosmos"
	CosmosDBURL       string
	CosmosDBName      string
	CosmosContainerName string
	ManagementClusterResourceID *azcorearm.ResourceID

	// DynamoDB fields — required when Backend == "dynamodb"
	DynamoTableName string
	AWSRegion       string
}

// NewKubeApplierDBClient constructs a KubeApplierDBClient for the backend
// specified in cfg.Backend.  For the Cosmos path it wraps the existing
// kubeApplierCosmosDBClient; for DynamoDB (and future backends) it wraps
// the new StorageBackend abstraction.
func NewKubeApplierDBClient(ctx context.Context, cfg BackendConfig) (database.KubeApplierDBClient, error) {
	switch cfg.Backend {
	case BackendTypeCosmos:
		return newCosmosKubeApplierDBClient(ctx, cfg)
	case BackendTypeDynamoDB:
		return newDynamoDBKubeApplierDBClient(ctx, cfg)
	default:
		return nil, fmt.Errorf("unknown backend %q: must be %q or %q", cfg.Backend, BackendTypeCosmos, BackendTypeDynamoDB)
	}
}

// newCosmosKubeApplierDBClient is the original Cosmos wiring, unchanged.
func newCosmosKubeApplierDBClient(ctx context.Context, cfg BackendConfig) (database.KubeApplierDBClient, error) {
	clientOptions := azcore.ClientOptions{Cloud: cloud.AzurePublic}
	cosmosDatabaseClient, err := database.NewCosmosDatabaseClient(cfg.CosmosDBURL, cfg.CosmosDBName, clientOptions)
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to create Azure Cosmos database client: %w", err))
	}
	client, err := database.NewKubeApplierDBClientFromDatabase(cosmosDatabaseClient, cfg.CosmosContainerName, cfg.ManagementClusterResourceID)
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to create KubeApplierDBClient: %w", err))
	}
	_ = ctx
	return client, nil
}

// newDynamoDBKubeApplierDBClient wires the DynamoDB StorageBackend.
func newDynamoDBKubeApplierDBClient(ctx context.Context, cfg BackendConfig) (database.KubeApplierDBClient, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.AWSRegion))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	backend := dynamobk.NewFromConfig(awsCfg, cfg.DynamoTableName)
	return database.NewKubeApplierBackendDBClient(backend, cfg.ManagementClusterResourceID), nil
}

// NewCosmosBackend constructs a Cosmos StorageBackend directly.
// Useful for callers that want to use StorageBackend-aware helpers against Cosmos.
func NewCosmosBackend(ctx context.Context, cosmosDBURL, cosmosDBName, containerName string) (database.StorageBackend, error) {
	clientOptions := azcore.ClientOptions{Cloud: cloud.AzurePublic}
	cosmosDatabaseClient, err := database.NewCosmosDatabaseClient(cosmosDBURL, cosmosDBName, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cosmos database client: %w", err)
	}
	container, err := cosmosDatabaseClient.NewContainer(containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to open Cosmos container: %w", err)
	}
	_ = ctx
	return cosmosbk.New(container), nil
}

// NewDynamoDBBackend constructs a DynamoDB StorageBackend.
func NewDynamoDBBackend(ctx context.Context, region, tableName string) (database.StorageBackend, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	return dynamobk.NewFromConfig(awsCfg, tableName), nil
}
