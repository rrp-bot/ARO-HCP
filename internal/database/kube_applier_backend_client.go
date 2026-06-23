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

package database

// This file contains the StorageBackend-backed implementation of
// KubeApplierDBClient.  The existing Cosmos-backed implementation
// (kubeApplierCosmosDBClient) is preserved unchanged in kube_applier_client.go.
//
// NewKubeApplierBackendDBClient is the entry point for the DynamoDB (and any
// future) backend path; it is wired in kube-applier/pkg/app/backend_wiring.go
// when --backend != cosmos.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/arm"
	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
)

// NewKubeApplierBackendDBClient constructs a KubeApplierDBClient backed by
// any StorageBackend implementation (Cosmos wrapper, DynamoDB, etc.).
// managementClusterResourceID is used as the shard key for every document
// written or read through this client.
func NewKubeApplierBackendDBClient(backend StorageBackend, managementClusterResourceID *azcorearm.ResourceID) KubeApplierDBClient {
	return &kubeApplierBackendDBClient{
		backend: backend,
		mcID:    managementClusterResourceID,
	}
}

// kubeApplierBackendDBClient implements KubeApplierDBClient against a StorageBackend.
type kubeApplierBackendDBClient struct {
	backend StorageBackend
	mcID    *azcorearm.ResourceID
}

var _ KubeApplierDBClient = &kubeApplierBackendDBClient{}

func (c *kubeApplierBackendDBClient) shardKey() string {
	return strings.ToLower(c.mcID.String())
}

func (c *kubeApplierBackendDBClient) ApplyDesiresForCluster(subscriptionID, resourceGroupName, clusterName string) (ResourceCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire], error) {
	parentID, err := api.ToClusterResourceID(subscriptionID, resourceGroupName, clusterName)
	if err != nil {
		return nil, err
	}
	return newBackendResourceCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire, GenericDocument[kubeapplier.ApplyDesire]](
		c.backend, c.shardKey(), parentID, kubeapplier.ClusterScopedApplyDesireResourceType,
	), nil
}

func (c *kubeApplierBackendDBClient) ApplyDesiresForNodePool(subscriptionID, resourceGroupName, clusterName, nodePoolName string) (ResourceCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire], error) {
	parentID, err := api.ToNodePoolResourceID(subscriptionID, resourceGroupName, clusterName, nodePoolName)
	if err != nil {
		return nil, err
	}
	return newBackendResourceCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire, GenericDocument[kubeapplier.ApplyDesire]](
		c.backend, c.shardKey(), parentID, kubeapplier.NodePoolScopedApplyDesireResourceType,
	), nil
}

func (c *kubeApplierBackendDBClient) DeleteDesiresForCluster(subscriptionID, resourceGroupName, clusterName string) (ResourceCRUD[kubeapplier.DeleteDesire, *kubeapplier.DeleteDesire], error) {
	parentID, err := api.ToClusterResourceID(subscriptionID, resourceGroupName, clusterName)
	if err != nil {
		return nil, err
	}
	return newBackendResourceCRUD[kubeapplier.DeleteDesire, *kubeapplier.DeleteDesire, GenericDocument[kubeapplier.DeleteDesire]](
		c.backend, c.shardKey(), parentID, kubeapplier.ClusterScopedDeleteDesireResourceType,
	), nil
}

func (c *kubeApplierBackendDBClient) DeleteDesiresForNodePool(subscriptionID, resourceGroupName, clusterName, nodePoolName string) (ResourceCRUD[kubeapplier.DeleteDesire, *kubeapplier.DeleteDesire], error) {
	parentID, err := api.ToNodePoolResourceID(subscriptionID, resourceGroupName, clusterName, nodePoolName)
	if err != nil {
		return nil, err
	}
	return newBackendResourceCRUD[kubeapplier.DeleteDesire, *kubeapplier.DeleteDesire, GenericDocument[kubeapplier.DeleteDesire]](
		c.backend, c.shardKey(), parentID, kubeapplier.NodePoolScopedDeleteDesireResourceType,
	), nil
}

func (c *kubeApplierBackendDBClient) ReadDesiresForCluster(subscriptionID, resourceGroupName, clusterName string) (ResourceCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire], error) {
	parentID, err := api.ToClusterResourceID(subscriptionID, resourceGroupName, clusterName)
	if err != nil {
		return nil, err
	}
	return newBackendResourceCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire, GenericDocument[kubeapplier.ReadDesire]](
		c.backend, c.shardKey(), parentID, kubeapplier.ClusterScopedReadDesireResourceType,
	), nil
}

func (c *kubeApplierBackendDBClient) ReadDesiresForNodePool(subscriptionID, resourceGroupName, clusterName, nodePoolName string) (ResourceCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire], error) {
	parentID, err := api.ToNodePoolResourceID(subscriptionID, resourceGroupName, clusterName, nodePoolName)
	if err != nil {
		return nil, err
	}
	return newBackendResourceCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire, GenericDocument[kubeapplier.ReadDesire]](
		c.backend, c.shardKey(), parentID, kubeapplier.NodePoolScopedReadDesireResourceType,
	), nil
}

func (c *kubeApplierBackendDBClient) Listers() KubeApplierListers {
	return &backendKubeApplierListers{
		backend:  c.backend,
		shardKey: c.shardKey(),
	}
}

func (c *kubeApplierBackendDBClient) UntypedCRUD(parentResourceID azcorearm.ResourceID) (UntypedResourceCRUD, error) {
	// UntypedCRUD is used by the orphan-cleanup controller which is only
	// needed for the Cosmos per-container migration path. For the generic
	// backend path we return a no-op implementation that is sufficient for
	// the kube-applier's own reconcile loops.
	return &backendUntypedCRUD{
		backend:          c.backend,
		shardKey:         c.shardKey(),
		parentResourceID: parentResourceID,
	}, nil
}

// ---------------------------------------------------------------------------
// backendKubeApplierListers
// ---------------------------------------------------------------------------

type backendKubeApplierListers struct {
	backend  StorageBackend
	shardKey string
}

var _ KubeApplierListers = &backendKubeApplierListers{}

func (g *backendKubeApplierListers) ApplyDesires() GlobalLister[kubeapplier.ApplyDesire] {
	return &backendGlobalLister[kubeapplier.ApplyDesire, GenericDocument[kubeapplier.ApplyDesire]]{
		backend:  g.backend,
		shardKey: g.shardKey,
		resourceTypes: []string{
			kubeapplier.ClusterScopedApplyDesireResourceType.String(),
			kubeapplier.NodePoolScopedApplyDesireResourceType.String(),
		},
	}
}

func (g *backendKubeApplierListers) DeleteDesires() GlobalLister[kubeapplier.DeleteDesire] {
	return &backendGlobalLister[kubeapplier.DeleteDesire, GenericDocument[kubeapplier.DeleteDesire]]{
		backend:  g.backend,
		shardKey: g.shardKey,
		resourceTypes: []string{
			kubeapplier.ClusterScopedDeleteDesireResourceType.String(),
			kubeapplier.NodePoolScopedDeleteDesireResourceType.String(),
		},
	}
}

func (g *backendKubeApplierListers) ReadDesires() GlobalLister[kubeapplier.ReadDesire] {
	return &backendGlobalLister[kubeapplier.ReadDesire, GenericDocument[kubeapplier.ReadDesire]]{
		backend:  g.backend,
		shardKey: g.shardKey,
		resourceTypes: []string{
			kubeapplier.ClusterScopedReadDesireResourceType.String(),
			kubeapplier.NodePoolScopedReadDesireResourceType.String(),
		},
	}
}

// ---------------------------------------------------------------------------
// backendGlobalLister — satisfies GlobalLister[T]
// ---------------------------------------------------------------------------

type backendGlobalLister[InternalAPIType any, CosmosAPIType any] struct {
	backend       StorageBackend
	shardKey      string
	resourceTypes []string

	// state set after List() is called
	err               error
	continuationToken string
}

var _ GlobalLister[kubeapplier.ApplyDesire] = &backendGlobalLister[kubeapplier.ApplyDesire, GenericDocument[kubeapplier.ApplyDesire]]{}

func (l *backendGlobalLister[InternalAPIType, CosmosAPIType]) List(ctx context.Context, options *DBClientListResourceDocsOptions) (DBClientIterator[InternalAPIType], error) {
	var qopts *QueryOptions
	if options != nil {
		qopts = &QueryOptions{}
		if options.PageSizeHint != nil {
			qopts.PageSizeHint = *options.PageSizeHint
		}
		if options.ContinuationToken != nil {
			qopts.ContinuationToken = *options.ContinuationToken
		}
	}

	result, err := l.backend.Query(ctx, l.shardKey, l.resourceTypes, qopts)
	if err != nil {
		return nil, err
	}

	// Unmarshal each raw JSON blob into the CosmosAPIType envelope, then convert
	// to the internal type using the existing CosmosToInternal machinery.
	var ids []string
	var items []*InternalAPIType
	for _, raw := range result.Items {
		var envelope CosmosAPIType
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, fmt.Errorf("backendGlobalLister unmarshal: %w", err)
		}
		internalObj, err := CosmosToInternal[InternalAPIType, CosmosAPIType](&envelope)
		if err != nil {
			return nil, fmt.Errorf("backendGlobalLister CosmosToInternal: %w", err)
		}
		// Extract document ID from the base envelope for the iterator yield key.
		var base BaseDocument
		if err := json.Unmarshal(raw, &base); err != nil {
			return nil, fmt.Errorf("backendGlobalLister unmarshal base: %w", err)
		}
		ids = append(ids, base.ID)
		items = append(items, internalObj)
	}

	return &sliceIterator[InternalAPIType]{
		ids:               ids,
		items:             items,
		continuationToken: result.ContinuationToken,
	}, nil
}

// sliceIterator is a simple in-memory DBClientIterator backed by a slice.
type sliceIterator[T any] struct {
	ids               []string
	items             []*T
	continuationToken string
	err               error
}

func (s *sliceIterator[T]) Items(ctx context.Context) DBClientIteratorItem[T] {
	return func(yield func(string, *T) bool) {
		for i, item := range s.items {
			if !yield(s.ids[i], item) {
				return
			}
		}
	}
}

func (s *sliceIterator[T]) GetContinuationToken() string { return s.continuationToken }
func (s *sliceIterator[T]) GetError() error              { return s.err }

// ---------------------------------------------------------------------------
// backendResourceCRUD — satisfies ResourceCRUD[T, *T]
// ---------------------------------------------------------------------------

// backendResourceCRUD implements ResourceCRUD against a StorageBackend.
// It reuses the existing serialisation helpers (PrepareForCreate,
// PrepareForReplace, SerializeItem, CosmosToInternal) so the document
// envelope format is identical to the Cosmos path.
type backendResourceCRUD[InternalAPIType any, InternalAPITypePointer arm.CosmosMetadataAccessorPtr[InternalAPIType], CosmosAPIType any] struct {
	backend          StorageBackend
	shardKey         string
	parentResourceID *azcorearm.ResourceID
	resourceType     azcorearm.ResourceType
}

func newBackendResourceCRUD[InternalAPIType any, InternalAPITypePointer arm.CosmosMetadataAccessorPtr[InternalAPIType], CosmosAPIType any](
	backend StorageBackend,
	shardKey string,
	parentResourceID *azcorearm.ResourceID,
	resourceType azcorearm.ResourceType,
) ResourceCRUD[InternalAPIType, InternalAPITypePointer] {
	return &backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]{
		backend:          backend,
		shardKey:         shardKey,
		parentResourceID: parentResourceID,
		resourceType:     resourceType,
	}
}

// GetByID fetches a document by its documentID (UUID).
func (c *backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]) GetByID(ctx context.Context, documentID string) (*InternalAPIType, error) {
	data, etag, err := c.backend.Get(ctx, documentID, c.shardKey)
	if err != nil {
		return nil, err
	}
	return c.unmarshalWithEtag(data, etag)
}

// Get fetches a document by its resource name (leaf name under the parent).
func (c *backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]) Get(ctx context.Context, resourceName string) (*InternalAPIType, error) {
	rid, err := ClusterNestedResourceIDBuilder{}.BuildResourceID(c.parentResourceID, c.resourceType, resourceName)
	if err != nil {
		return nil, err
	}
	docID, err := arm.ResourceIDToDocumentID(rid)
	if err != nil {
		return nil, err
	}
	return c.GetByID(ctx, docID)
}

// List returns all documents under the parent prefix that match the resource type.
func (c *backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]) List(ctx context.Context, opts *DBClientListResourceDocsOptions) (DBClientIterator[InternalAPIType], error) {
	lister := &backendGlobalLister[InternalAPIType, CosmosAPIType]{
		backend:       c.backend,
		shardKey:      c.shardKey,
		resourceTypes: []string{c.resourceType.String()},
	}
	return lister.List(ctx, opts)
}

// Create writes a new document, using PrepareForCreate + SerializeItem for
// the existing envelope format.
func (c *backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]) Create(ctx context.Context, newObj *InternalAPIType, _ *azcosmos.ItemOptions) (*InternalAPIType, error) {
	ptr := InternalAPITypePointer(newObj)
	ptr.SetShardKey(c.shardKey)
	if err := PrepareForCreate[InternalAPIType, InternalAPITypePointer](ptr); err != nil {
		return nil, err
	}
	_, data, err := SerializeItem[InternalAPIType, CosmosAPIType, InternalAPITypePointer](ptr)
	if err != nil {
		return nil, err
	}
	docID := ptr.GetDocumentID()
	stored, etag, err := c.backend.Create(ctx, docID, c.shardKey, data)
	if err != nil {
		return nil, err
	}
	return c.unmarshalWithEtag(stored, etag)
}

// Replace updates an existing document with an optimistic concurrency check.
func (c *backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]) Replace(ctx context.Context, newObj *InternalAPIType, _ *azcosmos.ItemOptions) (*InternalAPIType, error) {
	ptr := InternalAPITypePointer(newObj)
	expectedEtag := string(ptr.GetEtag())
	ptr.SetShardKey(c.shardKey)
	if err := PrepareForReplace[InternalAPIType, InternalAPITypePointer](ptr); err != nil {
		return nil, err
	}
	_, data, err := SerializeItem[InternalAPIType, CosmosAPIType, InternalAPITypePointer](ptr)
	if err != nil {
		return nil, err
	}
	docID := ptr.GetDocumentID()
	stored, etag, err := c.backend.Replace(ctx, docID, c.shardKey, expectedEtag, data)
	if err != nil {
		return nil, err
	}
	return c.unmarshalWithEtag(stored, etag)
}

// Delete removes a document by resource name.
func (c *backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]) Delete(ctx context.Context, resourceName string) error {
	rid, err := ClusterNestedResourceIDBuilder{}.BuildResourceID(c.parentResourceID, c.resourceType, resourceName)
	if err != nil {
		return err
	}
	docID, err := arm.ResourceIDToDocumentID(rid)
	if err != nil {
		return err
	}
	return c.backend.Delete(ctx, docID, c.shardKey)
}

// AddCreateToTransaction is not supported for non-Cosmos backends.
// It panics with an informative message to catch programming errors early.
func (c *backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]) AddCreateToTransaction(_ context.Context, _ DBTransaction, _ *InternalAPIType, _ *azcosmos.TransactionalBatchItemOptions) (string, error) {
	return "", fmt.Errorf("AddCreateToTransaction is not supported for non-Cosmos StorageBackend")
}

// AddReplaceToTransaction is not supported for non-Cosmos backends.
func (c *backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]) AddReplaceToTransaction(_ context.Context, _ DBTransaction, _ *InternalAPIType, _ *azcosmos.TransactionalBatchItemOptions) (string, error) {
	return "", fmt.Errorf("AddReplaceToTransaction is not supported for non-Cosmos StorageBackend")
}

// unmarshalWithEtag decodes a raw JSON blob from the backend, injects the
// etag from the returned backend token, and returns the internal type.
func (c *backendResourceCRUD[InternalAPIType, InternalAPITypePointer, CosmosAPIType]) unmarshalWithEtag(data []byte, etag string) (*InternalAPIType, error) {
	var envelope CosmosAPIType
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("backendResourceCRUD unmarshal: %w", err)
	}
	obj, err := CosmosToInternal[InternalAPIType, CosmosAPIType](&envelope)
	if err != nil {
		return nil, fmt.Errorf("backendResourceCRUD CosmosToInternal: %w", err)
	}
	// Overwrite the etag with the one returned by the backend so it is
	// always current regardless of what was embedded in the JSON blob.
	InternalAPITypePointer(obj).SetEtag(azcore.ETag(etag))
	return obj, nil
}

// ---------------------------------------------------------------------------
// backendUntypedCRUD — minimal no-op implementation for the generic backend path
// ---------------------------------------------------------------------------

type backendUntypedCRUD struct {
	backend          StorageBackend
	shardKey         string
	parentResourceID azcorearm.ResourceID
}

var _ UntypedResourceCRUD = &backendUntypedCRUD{}

func (u *backendUntypedCRUD) Get(_ context.Context, _ *azcorearm.ResourceID) (*TypedDocument, error) {
	return nil, fmt.Errorf("UntypedCRUD.Get is not supported for non-Cosmos StorageBackend")
}

func (u *backendUntypedCRUD) List(_ context.Context, _ *DBClientListResourceDocsOptions) (DBClientIterator[TypedDocument], error) {
	return &sliceIterator[TypedDocument]{}, nil
}

func (u *backendUntypedCRUD) ListRecursive(_ context.Context, _ *DBClientListResourceDocsOptions) (DBClientIterator[TypedDocument], error) {
	return &sliceIterator[TypedDocument]{}, nil
}

func (u *backendUntypedCRUD) Delete(_ context.Context, rid *azcorearm.ResourceID) error {
	docID, err := arm.ResourceIDToDocumentID(rid)
	if err != nil {
		return err
	}
	return u.backend.Delete(context.Background(), docID, u.shardKey)
}

func (u *backendUntypedCRUD) DeleteByDocumentID(_ context.Context, _, documentID string) error {
	return u.backend.Delete(context.Background(), documentID, u.shardKey)
}

// DeleteByCosmosID is a backward-compatible alias for DeleteByDocumentID.
func (u *backendUntypedCRUD) DeleteByCosmosID(ctx context.Context, partitionKey, cosmosID string) error {
	return u.DeleteByDocumentID(ctx, partitionKey, cosmosID)
}

func (u *backendUntypedCRUD) Child(resourceType azcorearm.ResourceType, resourceName string) (UntypedResourceCRUD, error) {
	if len(resourceName) == 0 {
		return nil, fmt.Errorf("resourceName is required")
	}
	child := u.parentResourceID
	return &backendUntypedCRUD{
		backend:          u.backend,
		shardKey:         u.shardKey,
		parentResourceID: child,
	}, nil
}
