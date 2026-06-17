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

package nodepoolcreationcontrollers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	arohcpv1alpha1 "github.com/openshift-online/ocm-sdk-go/arohcp/v1alpha1"
	ocmerrors "github.com/openshift-online/ocm-sdk-go/errors"

	"github.com/Azure/ARO-HCP/backend/pkg/controllers/controllerutils"
	"github.com/Azure/ARO-HCP/backend/pkg/informers"
	"github.com/Azure/ARO-HCP/backend/pkg/listers"
	"github.com/Azure/ARO-HCP/internal/api"
	controllerutil "github.com/Azure/ARO-HCP/internal/controllerutils"
	"github.com/Azure/ARO-HCP/internal/database"
	"github.com/Azure/ARO-HCP/internal/ocm"
	"github.com/Azure/ARO-HCP/internal/utils"
)

const NodePoolClusterServiceCreateControllerName = "NodePoolClusterServiceCreate"

type nodePoolClusterServiceCreateSyncer struct {
	cooldownChecker       controllerutil.CooldownChecker
	resourcesDBClient     database.ResourcesDBClient
	nodePoolLister        listers.NodePoolLister
	clusterLister         listers.ClusterLister
	controllerLister      listers.ControllerLister
	clustersServiceClient ocm.ClusterServiceClientSpec
}

func NewNodePoolClusterServiceCreateController(
	resourcesDBClient database.ResourcesDBClient,
	clustersServiceClient ocm.ClusterServiceClientSpec,
	activeOperationLister listers.ActiveOperationLister,
	informers informers.BackendInformers,
) controllerutils.Controller {
	_, nodePoolLister := informers.NodePools()
	_, clusterLister := informers.Clusters()
	_, controllerLister := informers.Controllers()
	syncer := &nodePoolClusterServiceCreateSyncer{
		cooldownChecker:       controllerutils.DefaultActiveOperationPrioritizingCooldown(activeOperationLister),
		resourcesDBClient:     resourcesDBClient,
		nodePoolLister:        nodePoolLister,
		clusterLister:         clusterLister,
		controllerLister:      controllerLister,
		clustersServiceClient: clustersServiceClient,
	}

	return controllerutils.NewNodePoolWatchingController(
		NodePoolClusterServiceCreateControllerName,
		resourcesDBClient,
		informers,
		time.Minute,
		syncer,
	)
}

func (c *nodePoolClusterServiceCreateSyncer) needsWork(ctx context.Context, nodePool *api.HCPOpenShiftClusterNodePool) bool {
	return nodePool.ServiceProviderProperties.DeletionTimestamp == nil &&
		(nodePool.ServiceProviderProperties.ClusterServiceID == nil || len(nodePool.ServiceProviderProperties.ClusterServiceID.String()) == 0)
}

func (c *nodePoolClusterServiceCreateSyncer) SyncOnce(ctx context.Context, key controllerutils.HCPNodePoolKey) error {
	logger := utils.LoggerFromContext(ctx)

	nodePool, err := c.nodePoolLister.Get(ctx, key.SubscriptionID, key.ResourceGroupName, key.HCPClusterName, key.HCPNodePoolName)
	if database.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return utils.TrackError(err)
	}

	if !c.needsWork(ctx, nodePool) {
		return nil
	}

	// For the NodePool, we retrieve from the actual database because we are about to use its data to interact with cluster-service.
	nodePool, err = c.resourcesDBClient.HCPClusters(key.SubscriptionID, key.ResourceGroupName).NodePools(key.HCPClusterName).Get(ctx, key.HCPNodePoolName)
	if database.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return utils.TrackError(err)
	}

	if !c.needsWork(ctx, nodePool) {
		return nil
	}

	intentFailed, err := c.isIntentFailed(ctx, key)
	if err != nil {
		return utils.TrackError(err)
	}
	if intentFailed {
		// If we failed permanently, we don't need to try again.
		return nil
	}

	// For the Cluster, we retrieve from the cache because we are not about to use its data to interact with cluster-service. At
	// the moment we only use the ClusterServiceID to interact with cluster-service, which shouldn't change over time once set.
	// If at some point this controller evolves to use other Cluster properties that will be sent to cluster-service and that
	// can change over time, we will need to retrieve from the database instead.
	cluster, err := c.clusterLister.Get(ctx, key.SubscriptionID, key.ResourceGroupName, key.HCPClusterName)
	if err != nil {
		return utils.TrackError(err)
	}
	if cluster.ServiceProviderProperties.ClusterServiceID == nil || len(cluster.ServiceProviderProperties.ClusterServiceID.String()) == 0 {
		return utils.TrackError(fmt.Errorf("cluster %s has no ClusterServiceID", key.HCPClusterName))
	}
	clusterCSInternalID := *cluster.ServiceProviderProperties.ClusterServiceID

	// GET must target the same href POST would use: {clusterHref}/node_pools/{id} where id is
	// lowercased ARM name (see ocm.BuildCSNodePool). We reconstruct it here:
	csNodePoolHREF := ocm.GenerateAROHCPNodePoolHREF(clusterCSInternalID.ID(), strings.ToLower(key.HCPNodePoolName))
	nodePoolCSInternalID, err := api.NewInternalID(csNodePoolHREF)
	if err != nil {
		return utils.TrackError(fmt.Errorf("build node pool internal ID for adoption lookup: %w", err))
	}

	existing, err := c.findCSNodePool(ctx, nodePoolCSInternalID)
	if err != nil {
		return utils.TrackError(err)
	}

	if existing == nil {
		csNodePoolBuilder, err := ocm.BuildCSNodePool(ctx, nodePool, false)
		if err != nil {
			return utils.TrackError(err)
		}
		logger.Info("performing POST node pool to Cluster Service", "cs_node_pool_href", csNodePoolHREF, "node_pool_resource_id", nodePool.ID.String())
		_, err = c.clustersServiceClient.PostNodePool(ctx, clusterCSInternalID, csNodePoolBuilder)
		if c.isOCMErrorBadRequest(err) {
			return c.setIntentFailed(ctx, key, err)
		}
		if err != nil {
			return utils.TrackError(err)
		}
	}

	logger.Info("setting node pool cluster service ID in node pool cosmos document", "node_pool_cluster_service_id", nodePoolCSInternalID.String())
	replacement := nodePool.DeepCopy()
	replacement.ServiceProviderProperties.ClusterServiceID = nodePoolCSInternalID.DeepCopy() // DeepCopy() to avoid referencing the original pointer
	_, err = c.resourcesDBClient.HCPClusters(key.SubscriptionID, key.ResourceGroupName).NodePools(key.HCPClusterName).Replace(ctx, replacement, nil)
	if database.IsPreconditionFailedError(err) {
		// if we have a conflict error, then we're guaranteed that our informer will eventually see an update and trigger us again.
		return nil
	}
	if err != nil {
		return utils.TrackError(err)
	}

	controllerCRUD := c.resourcesDBClient.HCPClusters(key.SubscriptionID, key.ResourceGroupName).NodePools(key.HCPClusterName).Controllers(key.HCPNodePoolName)
	if writeErr := controllerutils.WriteController(ctx, controllerCRUD, NodePoolClusterServiceCreateControllerName, key.InitialController,
		func(ctrl *api.Controller) {
			apimeta.SetStatusCondition(&ctrl.Status.Conditions, metav1.Condition{
				Type:    api.ControllerConditionTypeIntentFailed,
				Status:  metav1.ConditionFalse,
				Reason:  api.ControllerConditionReasonAsExpected,
				Message: "",
			})
		}); writeErr != nil {
		if database.IsPreconditionFailedError(writeErr) {
			return nil
		}
		return utils.TrackError(writeErr)
	}

	return nil
}

// findCSNodePool performs GetNodePool for the given Cluster Service node pool InternalID.
// It returns (nil, nil) when CS responds with 404.
func (c *nodePoolClusterServiceCreateSyncer) findCSNodePool(ctx context.Context, nodePoolInternalID api.InternalID) (*arohcpv1alpha1.NodePool, error) {
	np, err := c.clustersServiceClient.GetNodePool(ctx, nodePoolInternalID)
	if err != nil {
		var ocmErr *ocmerrors.Error
		if errors.As(err, &ocmErr) && ocmErr.Status() == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	return np, nil
}

func (c *nodePoolClusterServiceCreateSyncer) CooldownChecker() controllerutil.CooldownChecker {
	return c.cooldownChecker
}

func (c *nodePoolClusterServiceCreateSyncer) setIntentFailed(ctx context.Context, key controllerutils.HCPNodePoolKey, err error) error {
	logger := utils.LoggerFromContext(ctx)
	logger.Error(err, "CS create rejected permanently because of client error, persisting IntentFailed condition")
	controllerCRUD := c.resourcesDBClient.HCPClusters(key.SubscriptionID, key.ResourceGroupName).NodePools(key.HCPClusterName).Controllers(key.HCPNodePoolName)
	if writeErr := controllerutils.WriteController(ctx, controllerCRUD, NodePoolClusterServiceCreateControllerName, key.InitialController,
		func(ctrl *api.Controller) {
			apimeta.SetStatusCondition(&ctrl.Status.Conditions, metav1.Condition{
				Type:    api.ControllerConditionTypeIntentFailed,
				Status:  metav1.ConditionTrue,
				Reason:  api.CSNodePoolCreateClientErrorReason,
				Message: utils.ErrorMessageWithoutLineTracking(err),
			})
		}); writeErr != nil {
		if database.IsPreconditionFailedError(writeErr) {
			return nil
		}
		return utils.TrackError(writeErr)
	}
	return nil
}

func (c *nodePoolClusterServiceCreateSyncer) isIntentFailed(ctx context.Context, key controllerutils.HCPNodePoolKey) (bool, error) {
	controllerDoc, err := c.controllerLister.GetForNodePool(ctx, key.SubscriptionID, key.ResourceGroupName, key.HCPClusterName, key.HCPNodePoolName, NodePoolClusterServiceCreateControllerName)
	if database.IsNotFoundError(err) {
		// If the controller doesn't exist we consider the intent has not failed.
		return false, nil
	}
	if err != nil {
		return false, utils.TrackError(err)
	}
	cond := apimeta.FindStatusCondition(controllerDoc.Status.Conditions, api.ControllerConditionTypeIntentFailed)
	return cond != nil && cond.Status == metav1.ConditionTrue && cond.Reason == api.CSNodePoolCreateClientErrorReason, nil
}

func (c *nodePoolClusterServiceCreateSyncer) isOCMErrorBadRequest(err error) bool {
	var ocmErr *ocmerrors.Error
	return err != nil && errors.As(err, &ocmErr) && ocmErr.Status() == http.StatusBadRequest
}
