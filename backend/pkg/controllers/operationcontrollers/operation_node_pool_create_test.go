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

package operationcontrollers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilsclock "k8s.io/utils/clock"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	arohcpv1alpha1 "github.com/openshift-online/ocm-sdk-go/arohcp/v1alpha1"

	"github.com/Azure/ARO-HCP/backend/pkg/controllers/nodepoolcreationcontrollers"
	"github.com/Azure/ARO-HCP/backend/pkg/listertesting"
	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/arm"
	"github.com/Azure/ARO-HCP/internal/database"
	"github.com/Azure/ARO-HCP/internal/databasetesting"
	"github.com/Azure/ARO-HCP/internal/ocm"
	"github.com/Azure/ARO-HCP/internal/utils"
)

func TestOperationNodePoolCreate_SynchronizeOperation(t *testing.T) {
	defaultNodePool := func(fixture *nodePoolTestFixture) *api.HCPOpenShiftClusterNodePool {
		return fixture.newNodePool()
	}

	nodePoolWithoutCSID := func(fixture *nodePoolTestFixture) *api.HCPOpenShiftClusterNodePool {
		np := fixture.newNodePool()
		np.ServiceProviderProperties.ClusterServiceID = nil
		return np
	}

	nodePoolWithDeletionTimestamp := func(fixture *nodePoolTestFixture) *api.HCPOpenShiftClusterNodePool {
		np := fixture.newNodePool()
		np.ServiceProviderProperties.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		return np
	}

	newCSNodePoolCreateControllerDoc := func(conditions []metav1.Condition) *api.Controller {
		controllerResourceID := api.Must(azcorearm.ParseResourceID(
			"/subscriptions/" + testSubscriptionID +
				"/resourceGroups/" + testResourceGroupName +
				"/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/" + testClusterName +
				"/nodePools/" + testNodePoolName +
				"/hcpOpenShiftControllers/" + nodepoolcreationcontrollers.NodePoolClusterServiceCreateControllerName))
		return &api.Controller{
			CosmosMetadata: arm.CosmosMetadata{ResourceID: controllerResourceID, PartitionKey: strings.ToLower(controllerResourceID.SubscriptionID)},
			Status: api.ControllerStatus{
				Conditions: conditions,
			},
		}
	}

	setupCSNodePoolStatus := func(t *testing.T, mock *ocm.MockClusterServiceClientSpec, fixture *nodePoolTestFixture, state, msg string) {
		t.Helper()
		nodePoolStatusBuilder := arohcpv1alpha1.NewNodePoolStatus().
			State(arohcpv1alpha1.NewNodePoolState().NodePoolStateValue(state))
		if msg != "" {
			nodePoolStatusBuilder = nodePoolStatusBuilder.Message(msg)
		}
		nodePoolStatus, err := nodePoolStatusBuilder.Build()
		require.NoError(t, err)
		mock.EXPECT().
			GetNodePoolStatus(gomock.Any(), fixture.nodePoolInternalID).
			Return(nodePoolStatus, nil)
	}

	tests := []struct {
		name        string
		nodePool    func(fixture *nodePoolTestFixture) *api.HCPOpenShiftClusterNodePool
		controllers []*api.Controller
		setupCSMock func(t *testing.T, mock *ocm.MockClusterServiceClientSpec, fixture *nodePoolTestFixture)
		expectError bool
		verifyDB    func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient)
	}{
		{
			name:     "node pool ready transitions to succeeded",
			nodePool: defaultNodePool,
			setupCSMock: func(t *testing.T, mock *ocm.MockClusterServiceClientSpec, fixture *nodePoolTestFixture) {
				setupCSNodePoolStatus(t, mock, fixture, string(NodePoolStateReady), "")
			},
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateSucceeded, op.Status)

				nodePool, err := db.HCPClusters(testSubscriptionID, testResourceGroupName).NodePools(testClusterName).Get(ctx, testNodePoolName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateSucceeded, nodePool.Properties.ProvisioningState)
				assert.Empty(t, nodePool.ServiceProviderProperties.ActiveOperationID)
			},
		},
		{
			name:     "node pool installing transitions to provisioning",
			nodePool: defaultNodePool,
			setupCSMock: func(t *testing.T, mock *ocm.MockClusterServiceClientSpec, fixture *nodePoolTestFixture) {
				setupCSNodePoolStatus(t, mock, fixture, string(NodePoolStateInstalling), "")
			},
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateProvisioning, op.Status)

				nodePool, err := db.HCPClusters(testSubscriptionID, testResourceGroupName).NodePools(testClusterName).Get(ctx, testNodePoolName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateProvisioning, nodePool.Properties.ProvisioningState)
				assert.Equal(t, testOperationName, nodePool.ServiceProviderProperties.ActiveOperationID)
			},
		},
		{
			name:     "node pool error transitions to failed",
			nodePool: defaultNodePool,
			setupCSMock: func(t *testing.T, mock *ocm.MockClusterServiceClientSpec, fixture *nodePoolTestFixture) {
				setupCSNodePoolStatus(t, mock, fixture, string(NodePoolStateError), "node pool creation failed")
			},
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateFailed, op.Status)
				assert.NotNil(t, op.Error)
				assert.Equal(t, "node pool creation failed", op.Error.Message)

				nodePool, err := db.HCPClusters(testSubscriptionID, testResourceGroupName).NodePools(testClusterName).Get(ctx, testNodePoolName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateFailed, nodePool.Properties.ProvisioningState)
				assert.Empty(t, nodePool.ServiceProviderProperties.ActiveOperationID)
			},
		},
		{
			name:     "node pool pending stays accepted",
			nodePool: defaultNodePool,
			setupCSMock: func(t *testing.T, mock *ocm.MockClusterServiceClientSpec, fixture *nodePoolTestFixture) {
				setupCSNodePoolStatus(t, mock, fixture, string(NodePoolStatePending), "")
			},
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateAccepted, op.Status)
			},
		},
		{
			name:     "node pool validating stays accepted",
			nodePool: defaultNodePool,
			setupCSMock: func(t *testing.T, mock *ocm.MockClusterServiceClientSpec, fixture *nodePoolTestFixture) {
				setupCSNodePoolStatus(t, mock, fixture, string(NodePoolStateValidating), "")
			},
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateAccepted, op.Status)
			},
		},
		{
			name:     "ClusterServiceID nil and IntentFailed True transitions to Failed",
			nodePool: nodePoolWithoutCSID,
			controllers: []*api.Controller{
				newCSNodePoolCreateControllerDoc([]metav1.Condition{
					{
						Type:    api.ControllerConditionTypeIntentFailed,
						Status:  metav1.ConditionTrue,
						Reason:  api.CSNodePoolCreateClientErrorReason,
						Message: "Machine type not supported in this region",
					},
				}),
			},
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateFailed, op.Status)
				require.NotNil(t, op.Error)
				assert.Equal(t, "Machine type not supported in this region", op.Error.Message)
			},
		},
		{
			name:     "ClusterServiceID nil and no controller doc stays Accepted",
			nodePool: nodePoolWithoutCSID,
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateAccepted, op.Status)
			},
		},
		{
			name:     "ClusterServiceID nil and IntentFailed False stays Accepted",
			nodePool: nodePoolWithoutCSID,
			controllers: []*api.Controller{
				newCSNodePoolCreateControllerDoc([]metav1.Condition{
					{
						Type:   api.ControllerConditionTypeIntentFailed,
						Status: metav1.ConditionFalse,
						Reason: api.ControllerConditionReasonAsExpected,
					},
				}),
			},
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateAccepted, op.Status)
			},
		},
		{
			name:     "ClusterServiceID nil and controller exists without IntentFailed condition stays Accepted",
			nodePool: nodePoolWithoutCSID,
			controllers: []*api.Controller{
				newCSNodePoolCreateControllerDoc(nil),
			},
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateAccepted, op.Status)
			},
		},
		{
			name:     "ClusterServiceID nil and IntentFailed True with wrong reason stays Accepted",
			nodePool: nodePoolWithoutCSID,
			controllers: []*api.Controller{
				newCSNodePoolCreateControllerDoc([]metav1.Condition{
					{
						Type:    api.ControllerConditionTypeIntentFailed,
						Status:  metav1.ConditionTrue,
						Reason:  "CSCreateNotAccepted",
						Message: "legacy reason should not fail the operation",
					},
				}),
			},
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateAccepted, op.Status)
			},
		},
		{
			name:     "DeletionTimestamp set skips reconciliation",
			nodePool: nodePoolWithDeletionTimestamp,
			verifyDB: func(t *testing.T, ctx context.Context, db *databasetesting.MockResourcesDBClient) {
				op, err := db.Operations(testSubscriptionID).Get(ctx, testOperationName)
				require.NoError(t, err)
				assert.Equal(t, arm.ProvisioningStateAccepted, op.Status)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = utils.ContextWithLogger(ctx, testr.New(t))
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			fixture := newNodePoolTestFixture()
			cluster := fixture.newCluster()
			nodePool := tt.nodePool(fixture)
			operation := fixture.newOperation(database.OperationRequestCreate)

			resources := []any{cluster, nodePool, operation}
			for _, controllerDoc := range tt.controllers {
				resources = append(resources, controllerDoc)
			}

			mockResourcesDBClient, err := databasetesting.NewMockResourcesDBClientWithResources(ctx, resources)
			require.NoError(t, err)

			mockCSClient := ocm.NewMockClusterServiceClientSpec(ctrl)
			if tt.setupCSMock != nil {
				tt.setupCSMock(t, mockCSClient, fixture)
			}

			controller := &operationNodePoolCreate{
				clock:                utilsclock.RealClock{},
				resourcesDBClient:    mockResourcesDBClient,
				controllerLister:     &listertesting.DBControllerLister{ResourcesDBClient: mockResourcesDBClient},
				clusterServiceClient: mockCSClient,
				notificationClient:   nil,
			}

			err = controller.SynchronizeOperation(ctx, fixture.operationKey())
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.verifyDB != nil {
				tt.verifyDB(t, ctx, mockResourcesDBClient)
			}
		})
	}
}
