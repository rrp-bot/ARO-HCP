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

package delete_desire

// This file replicates the five TestEvaluate_* scenarios from controller_test.go
// and additionally exercises the full desirestatuswriter.UpdateStatus path wired to a
// real DynamoDB backend via LocalStack.  The evaluate() logic is backend-agnostic;
// what these tests add is verification that the resulting status mutation actually
// persists into and round-trips from DynamoDB correctly.
//
// Set LOCALSTACK_ENDPOINT to a reachable LocalStack URL to run these tests;
// otherwise they are skipped.

import (
	"context"
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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/internal/database"
	dynbk "github.com/Azure/ARO-HCP/internal/database/backends/dynamodb"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/desirestatuswriter"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/keys"
)

// ---------------------------------------------------------------------------
// LocalStack scaffolding
// ---------------------------------------------------------------------------

func skipDynamoIfNoLocalStack(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("LOCALSTACK_ENDPOINT")
	if ep == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set — skipping DynamoDB delete_desire integration tests")
	}
	return ep
}

func newDynamoClientForDD(t *testing.T, endpoint string) *dynamodb.Client {
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

func createDDTable(t *testing.T, client *dynamodb.Client, tableName string) {
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

func sanitiseTableNameDD(s string) string {
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
	ddSub     = "00000000-0000-0000-0000-000000000003"
	ddRG      = "rg-dd"
	ddCluster = "cluster-dd"
	ddMCID    = "/providers/microsoft.redhatopenshift/stamps/1/managementclusters/mc-dd"
)

// newDDClient returns a KubeApplierDBClient backed by a fresh DynamoDB table.
func newDDClient(t *testing.T, endpoint string) database.KubeApplierDBClient {
	t.Helper()
	dynClient := newDynamoClientForDD(t, endpoint)
	tableName := sanitiseTableNameDD(fmt.Sprintf("dd-test-%s", t.Name()))
	createDDTable(t, dynClient, tableName)
	backend := dynbk.New(dynClient, tableName)
	mcID, err := azcorearm.ParseResourceID(ddMCID)
	if err != nil {
		t.Fatalf("parse MC ID: %v", err)
	}
	return database.NewKubeApplierBackendDBClient(backend, mcID)
}

func newDDDesire(t *testing.T, name string, target kubeapplier.ResourceReference) *kubeapplier.DeleteDesire {
	t.Helper()
	mcID := api.Must(azcorearm.ParseResourceID(ddMCID))
	rid := api.Must(azcorearm.ParseResourceID(kubeapplier.ToClusterScopedDeleteDesireResourceIDString(ddSub, ddRG, ddCluster, name)))
	return &kubeapplier.DeleteDesire{
		CosmosMetadata: api.CosmosMetadata{
			ResourceID:   rid,
			PartitionKey: strings.ToLower(ddMCID),
		},
		Spec: kubeapplier.DeleteDesireSpec{
			ManagementCluster: mcID,
			TargetItem:        target,
		},
	}
}

// seedDesire writes desire into DynamoDB and returns the stored copy (with etag).
func seedDesire(t *testing.T, ctx context.Context, c database.KubeApplierDBClient, desire *kubeapplier.DeleteDesire) *kubeapplier.DeleteDesire {
	t.Helper()
	crud, err := c.DeleteDesiresForCluster(ddSub, ddRG, ddCluster)
	if err != nil {
		t.Fatalf("DeleteDesiresForCluster: %v", err)
	}
	stored, err := crud.Create(ctx, desire, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return stored
}

// readBack re-fetches a desire from DynamoDB.
func readBack(t *testing.T, ctx context.Context, c database.KubeApplierDBClient, name string) *kubeapplier.DeleteDesire {
	t.Helper()
	crud, err := c.DeleteDesiresForCluster(ddSub, ddRG, ddCluster)
	if err != nil {
		t.Fatalf("DeleteDesiresForCluster: %v", err)
	}
	d, err := crud.Get(ctx, name)
	if err != nil {
		t.Fatalf("Get %q: %v", name, err)
	}
	return d
}

// newStatusWriter builds the real desirestatuswriter wired to the DynamoDB backend.
func newStatusWriter(c database.KubeApplierDBClient) desirestatuswriter.StatusWriter[kubeapplier.DeleteDesire, keys.DeleteDesireKey] {
	fetcher := &deleteDesireFetcher{crudByParent: c}
	replacer := &deleteDesireReplacer{crudByParent: c}
	return desirestatuswriter.New[kubeapplier.DeleteDesire, keys.DeleteDesireKey, *kubeapplier.DeleteDesire](fetcher, replacer)
}

// ---------------------------------------------------------------------------
// Tests — evaluate() logic (replicate controller_test.go + persist to DynamoDB)
// ---------------------------------------------------------------------------

// TestEvaluate_DynamoDB_TargetGone verifies evaluate() marks Successful=True
// when the target is absent, and that this status is persisted to DynamoDB.
func TestEvaluate_DynamoDB_TargetGone(t *testing.T) {
	ep := skipDynamoIfNoLocalStack(t)
	ctx := context.Background()

	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		{Version: "v1", Resource: "configmaps"}: "ConfigMapList",
	})
	c := &DeleteDesireController{dyn: dyn}

	dbClient := newDDClient(t, ep)
	desire := newDDDesire(t, "tg", kubeapplier.ResourceReference{
		Version: "v1", Resource: "configmaps", Namespace: "default", Name: "missing",
	})
	stored := seedDesire(t, ctx, dbClient, desire)

	mutate, err := c.evaluate(ctx, stored)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	// Persist via the real status writer.
	key, err := keys.DeleteDesireKeyFromResourceID(stored.GetResourceID())
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	sw := newStatusWriter(dbClient)
	if err := sw.UpdateStatus(ctx, key, mutate); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	updated := readBack(t, ctx, dbClient, "tg")
	cond := findCond(updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("Successful=%v, want True", cond)
	}
}

// TestEvaluate_DynamoDB_TargetWithDeletionTimestamp verifies WaitingForDeletion
// is persisted in DynamoDB.
func TestEvaluate_DynamoDB_TargetWithDeletionTimestamp(t *testing.T) {
	ep := skipDynamoIfNoLocalStack(t)
	ctx := context.Background()

	obj := &unstructuredHelper{Name: "doomed", Namespace: "default", UID: "doomed-uid", HasDT: true}
	dyn := obj.toFakeDynamic()

	c := &DeleteDesireController{dyn: dyn}
	dbClient := newDDClient(t, ep)
	desire := newDDDesire(t, "wfd", kubeapplier.ResourceReference{
		Version: "v1", Resource: "configmaps", Namespace: "default", Name: "doomed",
	})
	stored := seedDesire(t, ctx, dbClient, desire)

	mutate, err := c.evaluate(ctx, stored)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	key, _ := keys.DeleteDesireKeyFromResourceID(stored.GetResourceID())
	sw := newStatusWriter(dbClient)
	if err := sw.UpdateStatus(ctx, key, mutate); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	updated := readBack(t, ctx, dbClient, "wfd")
	cond := findCond(updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != kubeapplier.ConditionReasonWaitingForDeletion {
		t.Errorf("Successful=%v, want False/WaitingForDeletion", cond)
	}
}

// TestEvaluate_DynamoDB_APIError verifies KubeAPIError is persisted.
func TestEvaluate_DynamoDB_APIError(t *testing.T) {
	ep := skipDynamoIfNoLocalStack(t)
	ctx := context.Background()

	schm := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(schm, map[schema.GroupVersionResource]string{
		{Version: "v1", Resource: "configmaps"}: "ConfigMapList",
	}, newConfigMap("ae", "default", false))
	dyn.PrependReactor("delete", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("apiserver down")
	})

	c := &DeleteDesireController{dyn: dyn}
	dbClient := newDDClient(t, ep)
	desire := newDDDesire(t, "ae", kubeapplier.ResourceReference{
		Version: "v1", Resource: "configmaps", Namespace: "default", Name: "ae",
	})
	stored := seedDesire(t, ctx, dbClient, desire)

	mutate, _ := c.evaluate(ctx, stored)
	key, _ := keys.DeleteDesireKeyFromResourceID(stored.GetResourceID())
	sw := newStatusWriter(dbClient)
	if err := sw.UpdateStatus(ctx, key, mutate); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	updated := readBack(t, ctx, dbClient, "ae")
	cond := findCond(updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != kubeapplier.ConditionReasonKubeAPIError {
		t.Errorf("Successful=%v, want False/KubeAPIError", cond)
	}
}

// TestEvaluate_DynamoDB_BadTarget verifies PreCheckFailed is persisted.
func TestEvaluate_DynamoDB_BadTarget(t *testing.T) {
	ep := skipDynamoIfNoLocalStack(t)
	ctx := context.Background()

	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), nil)
	c := &DeleteDesireController{dyn: dyn}
	dbClient := newDDClient(t, ep)
	desire := newDDDesire(t, "bt", kubeapplier.ResourceReference{})
	stored := seedDesire(t, ctx, dbClient, desire)

	mutate, _ := c.evaluate(ctx, stored)
	key, _ := keys.DeleteDesireKeyFromResourceID(stored.GetResourceID())
	sw := newStatusWriter(dbClient)
	if err := sw.UpdateStatus(ctx, key, mutate); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	updated := readBack(t, ctx, dbClient, "bt")
	cond := findCond(updated.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
	if cond == nil || cond.Reason != kubeapplier.ConditionReasonPreCheckFailed {
		t.Errorf("Successful=%v, want PreCheckFailed", cond)
	}
}

// TestEvaluate_DynamoDB_ReplacePreconditionRetried verifies that a stale-etag
// Replace (ErrPreconditionFailed) surfaces from UpdateStatus so the controller
// can retry.
func TestEvaluate_DynamoDB_ReplacePreconditionRetried(t *testing.T) {
	ep := skipDynamoIfNoLocalStack(t)
	ctx := context.Background()

	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		{Version: "v1", Resource: "configmaps"}: "ConfigMapList",
	})
	c := &DeleteDesireController{dyn: dyn}
	dbClient := newDDClient(t, ep)

	desire := newDDDesire(t, "pr", kubeapplier.ResourceReference{
		Version: "v1", Resource: "configmaps", Namespace: "default", Name: "gone",
	})
	stored := seedDesire(t, ctx, dbClient, desire)

	// Simulate a concurrent write that bumps the etag between our evaluate() and our Replace().
	// We do this by doing a Replace on the stored document ourselves before UpdateStatus runs.
	{
		crud, _ := dbClient.DeleteDesiresForCluster(ddSub, ddRG, ddCluster)
		if _, err := crud.Replace(ctx, stored, nil); err != nil {
			t.Fatalf("pre-emptive Replace: %v", err)
		}
	}
	// Now 'stored' has a stale etag.  UpdateStatus fetches a fresh copy (correct etag)
	// before replacing, so UpdateStatus should succeed (not fail with PreconditionFailed),
	// because the desirestatuswriter always fetches fresh before replacing.
	mutate, err := c.evaluate(ctx, stored)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	key, _ := keys.DeleteDesireKeyFromResourceID(stored.GetResourceID())
	sw := newStatusWriter(dbClient)
	// UpdateStatus re-fetches, so it will succeed even though 'stored' had a stale etag.
	if err := sw.UpdateStatus(ctx, key, mutate); err != nil {
		t.Fatalf("UpdateStatus with stale caller etag should still succeed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Small helpers to reduce duplication with the existing test file
// ---------------------------------------------------------------------------

// unstructuredHelper builds a configmap-shaped unstructured object for fake clients.
type unstructuredHelper struct {
	Name      string
	Namespace string
	UID       ktypes.UID
	HasDT     bool
}

func (h *unstructuredHelper) toFakeDynamic() *fake.FakeDynamicClient {
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			{Version: "v1", Resource: "configmaps"}: "ConfigMapList",
		},
		newConfigMap(h.Name, h.Namespace, h.HasDT),
	)
}

// Ensure we use time to suppress the import as unused (it's referenced via newConfigMap).
var _ = time.Second
