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

// Package integration provides end-to-end tests for the kube-applier
// controllers wired against a real DynamoDB backend (LocalStack) and two
// envtest-managed Kubernetes clusters.
//
// # Prerequisites
//
//   - LOCALSTACK_ENDPOINT env var must point to a running LocalStack instance
//     (e.g. http://localhost:4566).  Tests are skipped when absent.
//   - KUBEBUILDER_ASSETS must point to a directory containing etcd,
//     kube-apiserver, and kubectl binaries so envtest can start. Obtain with:
//     setup-envtest use -p path  (from sigs.k8s.io/controller-runtime/tools/setup-envtest)
//
// # What is tested
//
// Two independent envtest clusters are started:
//   - serviceEnv  – simulates the ARO-HCP service cluster.
//   - mgmtEnv     – the management cluster where apply/delete/read operations land.
//
// A DynamoDB table in LocalStack acts as the backing store.
package integration

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/internal/database"
	dynbk "github.com/Azure/ARO-HCP/internal/database/backends/dynamodb"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/delete_desire"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/keys"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/read_desire_kubernetes"
)

// ---------------------------------------------------------------------------
// Suite-level environment (set up once via TestMain)
// ---------------------------------------------------------------------------

type suiteEnv struct {
	serviceEnv         *envtest.Environment
	mgmtEnv            *envtest.Environment
	mgmtDyn            dynamic.Interface
	svcCfg             *rest.Config
	localstackEndpoint string
}

var suite *suiteEnv

func TestMain(m *testing.M) {
	ep := os.Getenv("LOCALSTACK_ENDPOINT")
	if ep == "" {
		// Individual tests will T.Skip(); run the harness anyway so the output
		// is well-formed.
		os.Exit(m.Run())
	}

	env := &suiteEnv{localstackEndpoint: ep}
	env.serviceEnv = &envtest.Environment{}
	env.mgmtEnv = &envtest.Environment{}

	var err error
	env.svcCfg, err = env.serviceEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start service envtest: %v\n", err)
		os.Exit(1)
	}

	mgmtCfg, err := env.mgmtEnv.Start()
	if err != nil {
		_ = env.serviceEnv.Stop()
		fmt.Fprintf(os.Stderr, "start mgmt envtest: %v\n", err)
		os.Exit(1)
	}
	env.mgmtDyn, err = dynamic.NewForConfig(mgmtCfg)
	if err != nil {
		_ = env.serviceEnv.Stop()
		_ = env.mgmtEnv.Stop()
		fmt.Fprintf(os.Stderr, "dynamic client for mgmt: %v\n", err)
		os.Exit(1)
	}

	suite = env
	code := m.Run()
	_ = env.serviceEnv.Stop()
	_ = env.mgmtEnv.Stop()
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Per-test helpers
// ---------------------------------------------------------------------------

func skipIfNotReady(t *testing.T) {
	t.Helper()
	if suite == nil {
		t.Skip("LOCALSTACK_ENDPOINT not set — skipping integration tests")
	}
}

func newIntegDynDB(t *testing.T) *dynamodb.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: suite.localstackEndpoint, HostnameImmutable: true}, nil
			}),
		),
	)
	require.NoError(t, err)
	return dynamodb.NewFromConfig(cfg)
}

func sanitise(s string) string {
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

func createIntegTable(t *testing.T, client *dynamodb.Client) string {
	t.Helper()
	tableName := sanitise(fmt.Sprintf("integ-%s", t.Name()))
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
	return tableName
}

const (
	integSub     = "00000000-0000-0000-0000-000000000010"
	integRG      = "rg-integ"
	integCluster = "cluster-integ"
	integMCID    = "/providers/microsoft.redhatopenshift/stamps/1/managementclusters/mc-integ"
)

func newIntegDBClient(t *testing.T) database.KubeApplierDBClient {
	t.Helper()
	dynClient := newIntegDynDB(t)
	tableName := createIntegTable(t, dynClient)
	backend := dynbk.New(dynClient, tableName)
	mcID, err := azcorearm.ParseResourceID(integMCID)
	require.NoError(t, err)
	return database.NewKubeApplierBackendDBClient(backend, mcID)
}

func integMCIDParsed(t *testing.T) *azcorearm.ResourceID {
	t.Helper()
	id, err := azcorearm.ParseResourceID(integMCID)
	require.NoError(t, err)
	return id
}

func findIntegCond(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}

func makeUnstructuredCM(name, namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Integration test: DeleteDesire → removes object from management cluster
// ---------------------------------------------------------------------------

// TestIntegration_DeleteDesire_RemovesObject pre-creates a ConfigMap on the
// management envtest cluster, seeds a DeleteDesire in DynamoDB, runs the
// delete_desire controller, and verifies the ConfigMap is gone and
// Successful=True is stored in DynamoDB.
func TestIntegration_DeleteDesire_RemovesObject(t *testing.T) {
	skipIfNotReady(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbClient := newIntegDBClient(t)
	cmGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

	// Pre-create the target ConfigMap on the management envtest cluster.
	_, err := suite.mgmtDyn.Resource(cmGVR).Namespace("default").Create(ctx,
		makeUnstructuredCM("integ-delete-cm", "default"), metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("pre-create ConfigMap on mgmt cluster: %v", err)
	}

	// Seed a DeleteDesire into DynamoDB.
	rid := api.Must(azcorearm.ParseResourceID(kubeapplier.ToClusterScopedDeleteDesireResourceIDString(
		integSub, integRG, integCluster, "dd-integ",
	)))
	desire := &kubeapplier.DeleteDesire{
		CosmosMetadata: api.CosmosMetadata{
			ResourceID:   rid,
			PartitionKey: strings.ToLower(integMCID),
		},
		Spec: kubeapplier.DeleteDesireSpec{
			ManagementCluster: integMCIDParsed(t),
			TargetItem: kubeapplier.ResourceReference{
				Group: "", Version: "v1", Resource: "configmaps",
				Namespace: "default", Name: "integ-delete-cm",
			},
		},
	}
	ddCRUD, err := dbClient.DeleteDesiresForCluster(integSub, integRG, integCluster)
	require.NoError(t, err)
	stored, err := ddCRUD.Create(ctx, desire, nil)
	require.NoError(t, err)

	// Build a minimal static informer so the controller's event-handler
	// registration succeeds without needing a live apiserver informer.
	// We use a RaceFreeFake watcher and send a Bookmark event after the
	// informer starts — that is what causes the reflector to mark the
	// cache as synced (HasSynced=true).
	fakeWatcher := watch.NewRaceFreeFake()
	informer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return &kubeapplier.DeleteDesireList{Items: []kubeapplier.DeleteDesire{*stored}}, nil
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return fakeWatcher, nil
			},
		},
		&kubeapplier.DeleteDesire{},
		0,
		cache.Indexers{},
	)

	c, err := delete_desire.NewDeleteDesireController(informer, suite.mgmtDyn, dbClient, delete_desire.Config{
		CooldownPeriod: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	// Start the informer, send a Bookmark to unblock HasSynced, then wait.
	go informer.Run(ctx.Done())
	// Give the reflector time to issue the Watch call before we send on it.
	time.Sleep(100 * time.Millisecond)
	fakeWatcher.Action(watch.Bookmark, &kubeapplier.DeleteDesire{})
	syncCtx, syncCancel := context.WithTimeout(ctx, 5*time.Second)
	defer syncCancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced) {
		t.Fatal("informer did not sync within 5s")
	}

	// Run the controller.
	go c.Run(ctx, 1)

	// Wait for the ConfigMap to disappear from the management cluster.
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		_, getErr := suite.mgmtDyn.Resource(cmGVR).Namespace("default").Get(ctx, "integ-delete-cm", metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return true, nil
		}
		if getErr != nil {
			return false, getErr
		}
		return false, nil
	})
	assert.NoError(t, err, "ConfigMap should be removed from management cluster after DeleteDesire reconciles")

	// Also verify Successful=True is persisted to DynamoDB.
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		d, getErr := ddCRUD.Get(ctx, "dd-integ")
		if getErr != nil {
			return false, getErr
		}
		cond := findIntegCond(d.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
		return cond != nil && cond.Status == metav1.ConditionTrue, nil
	})
	assert.NoError(t, err, "Successful=True should be persisted to DynamoDB after deletion")
}

// ---------------------------------------------------------------------------
// Integration test: ReadDesire → mirrors ConfigMap status to DynamoDB
// ---------------------------------------------------------------------------

// TestIntegration_ReadDesire_MirrorsStatusToDynamoDB pre-creates a ConfigMap
// on the management envtest cluster, seeds a ReadDesire in DynamoDB, runs the
// read_desire_kubernetes controller until KubeContent is populated, and asserts
// the content reflects the ConfigMap's body.
func TestIntegration_ReadDesire_MirrorsStatusToDynamoDB(t *testing.T) {
	skipIfNotReady(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbClient := newIntegDBClient(t)
	cmGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

	// Pre-create the target ConfigMap.
	_, err := suite.mgmtDyn.Resource(cmGVR).Namespace("default").Create(ctx,
		makeUnstructuredCM("integ-read-cm", "default"), metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("pre-create ConfigMap on mgmt cluster: %v", err)
	}

	// Seed a ReadDesire into DynamoDB.
	rid := api.Must(azcorearm.ParseResourceID(kubeapplier.ToClusterScopedReadDesireResourceIDString(
		integSub, integRG, integCluster, "rd-integ",
	)))
	target := kubeapplier.ResourceReference{
		Group: "", Version: "v1", Resource: "configmaps",
		Namespace: "default", Name: "integ-read-cm",
	}
	desire := &kubeapplier.ReadDesire{
		CosmosMetadata: api.CosmosMetadata{
			ResourceID:   rid,
			PartitionKey: strings.ToLower(integMCID),
		},
		Spec: kubeapplier.ReadDesireSpec{
			ManagementCluster: integMCIDParsed(t),
			TargetItem:        target,
		},
	}
	rdCRUD, err := dbClient.ReadDesiresForCluster(integSub, integRG, integCluster)
	require.NoError(t, err)
	stored, err := rdCRUD.Create(ctx, desire, nil)
	require.NoError(t, err)

	// Build and run the controller. NewReadDesireKubernetesController starts its
	// own internal informer when Run() is called.
	key, err := keys.ReadDesireKeyFromResourceID(stored.GetResourceID())
	require.NoError(t, err)
	c, err := read_desire_kubernetes.NewReadDesireKubernetesController(key, target, suite.mgmtDyn, dbClient)
	require.NoError(t, err)

	go c.Run(ctx)

	// Poll DynamoDB until KubeContent is populated.
	err = wait.PollUntilContextTimeout(ctx, 300*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		d, getErr := rdCRUD.Get(ctx, "rd-integ")
		if getErr != nil {
			return false, getErr
		}
		return d.Status.KubeContent != nil && len(d.Status.KubeContent.Raw) > 0, nil
	})
	require.NoError(t, err, "KubeContent should be populated in DynamoDB within 30s")

	// Read the final state and assert.
	final, err := rdCRUD.Get(ctx, "rd-integ")
	require.NoError(t, err)
	assert.Contains(t, string(final.Status.KubeContent.Raw), "ConfigMap",
		"KubeContent should contain the ConfigMap body")
	cond := findIntegCond(final.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
}

// ---------------------------------------------------------------------------
// Integration test: Two clusters are isolated
// ---------------------------------------------------------------------------

// TestIntegration_TwoClusters_AreIsolated creates a ConfigMap on the service
// envtest cluster and verifies it is absent from the management cluster,
// confirming the two envtest environments are independent.
func TestIntegration_TwoClusters_AreIsolated(t *testing.T) {
	skipIfNotReady(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svcDyn, err := dynamic.NewForConfig(suite.svcCfg)
	require.NoError(t, err)

	cmGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

	// Create on service cluster.
	_, err = svcDyn.Resource(cmGVR).Namespace("default").Create(ctx,
		makeUnstructuredCM("svc-only-cm", "default"), metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create on service cluster: %v", err)
	}

	// Should NOT be present on management cluster.
	_, err = suite.mgmtDyn.Resource(cmGVR).Namespace("default").Get(ctx, "svc-only-cm", metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err),
		"ConfigMap created on service cluster must not exist on management cluster")
}
