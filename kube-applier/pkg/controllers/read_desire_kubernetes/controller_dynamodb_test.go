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

package read_desire_kubernetes

// This file contains DynamoDB-backed variants of the TestSyncOnce_* tests.
// The existing controller_test.go uses databasetesting.MockKubeApplierDBClient and
// a recordingWriter that intercepts status updates without persisting them.
// Here, we wire the real DynamoDB StorageBackend (via LocalStack) and let the
// real desirestatuswriter persist and retrieve from DynamoDB.
//
// Set LOCALSTACK_ENDPOINT to run these tests.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/internal/database"
	dynbk "github.com/Azure/ARO-HCP/internal/database/backends/dynamodb"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/keys"
)

// ---------------------------------------------------------------------------
// LocalStack scaffolding
// ---------------------------------------------------------------------------

func skipRDKIfNoLocalStack(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("LOCALSTACK_ENDPOINT")
	if ep == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set — skipping DynamoDB read_desire_kubernetes integration tests")
	}
	return ep
}

func newRDKDynClient(t *testing.T, endpoint string) *dynamodb.Client {
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
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}
	return dynamodb.NewFromConfig(cfg)
}

func createRDKTable(t *testing.T, client *dynamodb.Client, tableName string) {
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
	if err != nil {
		t.Fatalf("CreateTable %q: %v", tableName, err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})
}

func sanitiseRDKTableName(s string) string {
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

const (
	rdkSub     = "00000000-0000-0000-0000-000000000004"
	rdkRG      = "rg-rdk"
	rdkCluster = "cluster-rdk"
	rdkMCID    = "/providers/microsoft.redhatopenshift/stamps/1/managementclusters/mc-rdk"
)

// newRDKDBClient returns a KubeApplierDBClient backed by a fresh DynamoDB table.
func newRDKDBClient(t *testing.T, endpoint string) database.KubeApplierDBClient {
	t.Helper()
	dynClient := newRDKDynClient(t, endpoint)
	tableName := sanitiseRDKTableName(fmt.Sprintf("rdk-test-%s", t.Name()))
	createRDKTable(t, dynClient, tableName)
	backend := dynbk.New(dynClient, tableName)
	mcID, err := azcorearm.ParseResourceID(rdkMCID)
	if err != nil {
		t.Fatalf("parse MC ID: %v", err)
	}
	return database.NewKubeApplierBackendDBClient(backend, mcID)
}

func newRDKDesire(t *testing.T, name string, target kubeapplier.ResourceReference) *kubeapplier.ReadDesire {
	t.Helper()
	mcID := api.Must(azcorearm.ParseResourceID(rdkMCID))
	rid := api.Must(azcorearm.ParseResourceID(kubeapplier.ToClusterScopedReadDesireResourceIDString(rdkSub, rdkRG, rdkCluster, name)))
	return &kubeapplier.ReadDesire{
		CosmosMetadata: api.CosmosMetadata{
			ResourceID:   rid,
			PartitionKey: strings.ToLower(rdkMCID),
		},
		Spec: kubeapplier.ReadDesireSpec{
			ManagementCluster: mcID,
			TargetItem:        target,
		},
	}
}

// seedRDKDesire writes a ReadDesire to DynamoDB and returns the stored copy.
func seedRDKDesire(t *testing.T, ctx context.Context, c database.KubeApplierDBClient, desire *kubeapplier.ReadDesire) *kubeapplier.ReadDesire {
	t.Helper()
	crud, err := c.ReadDesiresForCluster(rdkSub, rdkRG, rdkCluster)
	if err != nil {
		t.Fatalf("ReadDesiresForCluster: %v", err)
	}
	stored, err := crud.Create(ctx, desire, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return stored
}

// readBackRDK fetches a ReadDesire from DynamoDB.
func readBackRDK(t *testing.T, ctx context.Context, c database.KubeApplierDBClient, name string) *kubeapplier.ReadDesire {
	t.Helper()
	crud, err := c.ReadDesiresForCluster(rdkSub, rdkRG, rdkCluster)
	if err != nil {
		t.Fatalf("ReadDesiresForCluster: %v", err)
	}
	d, err := crud.Get(ctx, name)
	if err != nil {
		t.Fatalf("Get %q: %v", name, err)
	}
	return d
}

// startSyncedControllerWithDynamo builds the controller wired to the real DynamoDB
// backend and starts its informer.  The controller's writer is NOT replaced with a
// recorder — it uses the real desirestatuswriter so status is persisted to DynamoDB.
func startSyncedControllerWithDynamo(
	t *testing.T,
	ctx context.Context,
	dbClient database.KubeApplierDBClient,
	target kubeapplier.ResourceReference,
	desire *kubeapplier.ReadDesire,
) *ReadDesireKubernetesController {
	t.Helper()

	// Seed the desire into DynamoDB so the fetcher can retrieve it.
	stored := seedRDKDesire(t, ctx, dbClient, desire)

	key, err := keys.ReadDesireKeyFromResourceID(stored.GetResourceID())
	if err != nil {
		t.Fatalf("derive key: %v", err)
	}

	dyn := dynamicForTestdata(t, "testdata/configmap_present")
	c, err := NewReadDesireKubernetesController(key, target, dyn, dbClient)
	if err != nil {
		t.Fatalf("NewReadDesireKubernetesController: %v", err)
	}

	go c.informer.RunWithContext(ctx)
	syncCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), c.informer.HasSynced) {
		t.Fatal("informer did not sync within 5s")
	}
	return c
}

// ---------------------------------------------------------------------------
// DynamoDB-backed replicas of the TestSyncOnce_* tests
// ---------------------------------------------------------------------------

// TestSyncOnce_DynamoDB_TargetExists replicates TestSyncOnce_TargetExists_PopulatesKubeContent
// but stores the ReadDesire in DynamoDB and reads back the persisted status.
func TestSyncOnce_DynamoDB_TargetExists(t *testing.T) {
	ep := skipRDKIfNoLocalStack(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbClient := newRDKDBClient(t, ep)
	target := kubeapplier.ResourceReference{
		Group: "", Version: "v1", Resource: "configmaps", Namespace: testTargetNs, Name: "hello",
	}
	desire := newRDKDesire(t, "rdk-exist", target)

	c := startSyncedControllerWithDynamo(t, ctx, dbClient, target, desire)
	if err := c.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	updated := readBackRDK(t, ctx, dbClient, "rdk-exist")
	if updated.Status.KubeContent == nil || len(updated.Status.KubeContent.Raw) == 0 {
		t.Fatal("KubeContent is empty after SyncOnce — status was not persisted to DynamoDB")
	}
	var got map[string]any
	if err := json.Unmarshal(updated.Status.KubeContent.Raw, &got); err != nil {
		t.Fatalf("unmarshal kubeContent: %v", err)
	}
	if got["kind"] != "ConfigMap" {
		t.Errorf("kind = %v, want ConfigMap", got["kind"])
	}
	cond := findCond(updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("Successful=%v, want True", cond)
	}
}

// TestSyncOnce_DynamoDB_TargetAbsent replicates TestSyncOnce_TargetAbsent_ReportsSuccessful
// using a real DynamoDB backend (testdata/configmap_absent returns a 404).
func TestSyncOnce_DynamoDB_TargetAbsent(t *testing.T) {
	ep := skipRDKIfNoLocalStack(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbClient := newRDKDBClient(t, ep)
	target := kubeapplier.ResourceReference{
		Group: "", Version: "v1", Resource: "configmaps", Namespace: testTargetNs, Name: "missing",
	}
	desire := newRDKDesire(t, "rdk-absent", target)

	stored := seedRDKDesire(t, ctx, dbClient, desire)
	key, err := keys.ReadDesireKeyFromResourceID(stored.GetResourceID())
	if err != nil {
		t.Fatalf("derive key: %v", err)
	}

	// Use the "absent" testdata directory.
	dyn := dynamicForTestdata(t, "testdata/configmap_absent")
	c, err := NewReadDesireKubernetesController(key, target, dyn, dbClient)
	if err != nil {
		t.Fatalf("NewReadDesireKubernetesController: %v", err)
	}

	go c.informer.RunWithContext(ctx)
	syncCtx, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	if !cache.WaitForCacheSync(syncCtx.Done(), c.informer.HasSynced) {
		t.Fatal("informer did not sync within 5s")
	}

	if err := c.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	updated := readBackRDK(t, ctx, dbClient, "rdk-absent")
	if updated.Status.KubeContent != nil {
		t.Errorf("KubeContent should be nil when target is absent, got %s", updated.Status.KubeContent.Raw)
	}
	cond := findCond(updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("Successful=%v, want True", cond)
	}
}
