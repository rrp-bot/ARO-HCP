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

// Package database_test contains LocalStack-backed integration tests for the
// KubeApplierDBClient built on the DynamoDB StorageBackend.  Set
// LOCALSTACK_ENDPOINT to a reachable LocalStack URL to run these tests;
// otherwise they are skipped.
package database_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/internal/database"
	dynbk "github.com/Azure/ARO-HCP/internal/database/backends/dynamodb"
)

// ---------------------------------------------------------------------------
// Shared test scaffolding
// ---------------------------------------------------------------------------

func skipBackendIfNoLocalStack(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("LOCALSTACK_ENDPOINT")
	if ep == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set — skipping DynamoDB backend client integration tests")
	}
	return ep
}

func newDynamoClientForBackendTest(t *testing.T, endpoint string) *dynamodb.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint, HostnameImmutable: true}, nil
			}),
		),
	)
	require.NoError(t, err)
	return dynamodb.NewFromConfig(cfg)
}

func createBackendTable(t *testing.T, client *dynamodb.Client, tableName string) {
	t.Helper()
	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(dynbk.AttributeShardKey), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(dynbk.AttributeDocumentID), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(dynbk.AttributeShardKey), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(dynbk.AttributeDocumentID), KeyType: types.KeyTypeRange},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})
}

// sanitiseTableName replaces characters invalid for DynamoDB table names.
func sanitiseTableName(s string) string {
	var buf strings.Builder
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			buf.WriteRune(ch)
		} else {
			buf.WriteRune('-')
		}
	}
	return buf.String()
}

// newKubeApplierClientUnderTest creates a DynamoDB table and returns a
// KubeApplierDBClient backed by it.
func newKubeApplierClientUnderTest(t *testing.T, endpoint, mcIDStr string) database.KubeApplierDBClient {
	t.Helper()
	client := newDynamoClientForBackendTest(t, endpoint)
	tableName := sanitiseTableName(fmt.Sprintf("ka-test-%s", t.Name()))
	createBackendTable(t, client, tableName)
	backend := dynbk.New(client, tableName)
	mcID, err := azcorearm.ParseResourceID(mcIDStr)
	require.NoError(t, err)
	return database.NewKubeApplierBackendDBClient(backend, mcID)
}

const (
	testBackendSub      = "00000000-0000-0000-0000-000000000002"
	testBackendRG       = "rg-test"
	testBackendCluster  = "cluster-test"
	testBackendMCID     = "/providers/microsoft.redhatopenshift/stamps/1/managementclusters/mc-test"
)

func newDeleteDesireForBackendTest(t *testing.T, name string) *kubeapplier.DeleteDesire {
	t.Helper()
	mcID, err := azcorearm.ParseResourceID(testBackendMCID)
	require.NoError(t, err)
	rid, err := azcorearm.ParseResourceID(kubeapplier.ToClusterScopedDeleteDesireResourceIDString(
		testBackendSub, testBackendRG, testBackendCluster, name,
	))
	require.NoError(t, err)
	return &kubeapplier.DeleteDesire{
		CosmosMetadata: api.CosmosMetadata{
			ResourceID:   rid,
			PartitionKey: strings.ToLower(testBackendMCID),
		},
		Spec: kubeapplier.DeleteDesireSpec{
			ManagementCluster: mcID,
			TargetItem: kubeapplier.ResourceReference{
				Version:   "v1",
				Resource:  "configmaps",
				Namespace: "default",
				Name:      name,
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestBackendClient_DeleteDesire_CreateGetDelete exercises the Create/Get/Delete
// CRUD cycle for DeleteDesires through the KubeApplierDBClient.
func TestBackendClient_DeleteDesire_CreateGetDelete(t *testing.T) {
	ep := skipBackendIfNoLocalStack(t)
	c := newKubeApplierClientUnderTest(t, ep, testBackendMCID)
	ctx := context.Background()

	crud, err := c.DeleteDesiresForCluster(testBackendSub, testBackendRG, testBackendCluster)
	require.NoError(t, err)

	desire := newDeleteDesireForBackendTest(t, "d1")
	created, err := crud.Create(ctx, desire, nil)
	require.NoError(t, err)
	assert.NotNil(t, created)
	assert.NotEmpty(t, string(created.GetEtag()), "Create should return a non-empty etag")

	// Get by resource name.
	fetched, err := crud.Get(ctx, "d1")
	require.NoError(t, err)
	assert.Equal(t, "d1", fetched.GetResourceID().Name)
	assert.NotEmpty(t, string(fetched.GetEtag()), "Get should return a non-empty etag")

	// Delete removes the document.
	err = crud.Delete(ctx, "d1")
	require.NoError(t, err)

	// Second delete is idempotent.
	err = crud.Delete(ctx, "d1")
	require.NoError(t, err)
}

// TestBackendClient_DeleteDesire_Replace verifies optimistic concurrency via Replace.
func TestBackendClient_DeleteDesire_Replace(t *testing.T) {
	ep := skipBackendIfNoLocalStack(t)
	c := newKubeApplierClientUnderTest(t, ep, testBackendMCID)
	ctx := context.Background()

	crud, err := c.DeleteDesiresForCluster(testBackendSub, testBackendRG, testBackendCluster)
	require.NoError(t, err)

	desire := newDeleteDesireForBackendTest(t, "rep1")
	created, err := crud.Create(ctx, desire, nil)
	require.NoError(t, err)

	// Replace with the current etag should succeed.
	replaced, err := crud.Replace(ctx, created, nil)
	require.NoError(t, err)
	assert.NotEqual(t, string(created.GetEtag()), string(replaced.GetEtag()), "Replace must assign a new etag")

	// Replace with a stale etag should fail (ErrPreconditionFailed or wrapped).
	_, err = crud.Replace(ctx, created, nil) // created still holds old etag
	assert.Error(t, err, "Replace with stale etag should fail")
}

// TestBackendClient_Listers_ApplyDesires checks that the lister returns items
// created via the CRUD layer.
func TestBackendClient_Listers_ApplyDesires(t *testing.T) {
	ep := skipBackendIfNoLocalStack(t)
	c := newKubeApplierClientUnderTest(t, ep, testBackendMCID)
	ctx := context.Background()

	mcID, err := azcorearm.ParseResourceID(testBackendMCID)
	require.NoError(t, err)

	// Create two ApplyDesires.
	for _, name := range []string{"ad1", "ad2"} {
		crud, err := c.ApplyDesiresForCluster(testBackendSub, testBackendRG, testBackendCluster)
		require.NoError(t, err)

		rid, err := azcorearm.ParseResourceID(kubeapplier.ToClusterScopedApplyDesireResourceIDString(
			testBackendSub, testBackendRG, testBackendCluster, name,
		))
		require.NoError(t, err)
		desire := &kubeapplier.ApplyDesire{
			CosmosMetadata: api.CosmosMetadata{
				ResourceID:   rid,
				PartitionKey: strings.ToLower(testBackendMCID),
			},
			Spec: kubeapplier.ApplyDesireSpec{
				ManagementCluster: mcID,
			},
		}
		_, err = crud.Create(ctx, desire, nil)
		require.NoError(t, err, "Create ApplyDesire %q", name)
	}

	// The lister must find both.
	iter, err := c.Listers().ApplyDesires().List(ctx, nil)
	require.NoError(t, err)

	var count int
	for _, item := range iter.Items(ctx) {
		_ = item
		count++
	}
	assert.Equal(t, 2, count, "lister should return 2 ApplyDesires")
	require.NoError(t, iter.GetError())
}

// TestBackendClient_Listers_Isolation verifies that DeleteDesires and
// ApplyDesires use distinct resource-type filters so one lister never returns
// the other's items.
func TestBackendClient_Listers_Isolation(t *testing.T) {
	ep := skipBackendIfNoLocalStack(t)
	c := newKubeApplierClientUnderTest(t, ep, testBackendMCID)
	ctx := context.Background()

	// Create one DeleteDesire.
	ddCRUD, err := c.DeleteDesiresForCluster(testBackendSub, testBackendRG, testBackendCluster)
	require.NoError(t, err)
	_, err = ddCRUD.Create(ctx, newDeleteDesireForBackendTest(t, "iso1"), nil)
	require.NoError(t, err)

	// ApplyDesires lister must return zero items (no ApplyDesires were created).
	applyIter, err := c.Listers().ApplyDesires().List(ctx, nil)
	require.NoError(t, err)
	var applyCount int
	for _, ad := range applyIter.Items(ctx) {
		_ = ad
		applyCount++
	}
	assert.Equal(t, 0, applyCount, "ApplyDesires lister must not return DeleteDesires")

	// DeleteDesires lister must find the one we created.
	delIter, err := c.Listers().DeleteDesires().List(ctx, nil)
	require.NoError(t, err)
	var delCount int
	for _, dd := range delIter.Items(ctx) {
		_ = dd
		delCount++
	}
	assert.Equal(t, 1, delCount, "DeleteDesires lister should find exactly 1 item")
}
