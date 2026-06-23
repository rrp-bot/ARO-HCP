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

package arm

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
)

// DocumentMetadata contains the information that persisted resources must have for us to support CRUD against them.
// These are not (currently) all stored in the same place in our various types.
// It is backend-agnostic: the same struct is used whether the backing store is
// Cosmos DB, DynamoDB, or any other StorageBackend implementation.
type DocumentMetadata struct {
	ResourceID *azcorearm.ResourceID `json:"resourceID"`

	// ExistingCosmosUID exists to allow for a migration path from where we are today to a uuid based documentID
	// and this will be deleted afterwards.
	ExistingCosmosUID string `json:"-"`

	// CosmosETag / Etag is used for optimistic concurrency. The storage backend populates
	// this on reads and checks it on conditional writes.
	CosmosETag azcore.ETag `json:"etag,omitempty"`

	// InstanceVersion is a field that auto-increments every time the resource is updated.  This gives us the ability to
	// compare two instances of stored resources and determine which one is newer.  We will use this field to integrate
	// changefeeds for controllers and decide which changes are newer than the level we have already observed so that we
	// can periodically re-list all items.
	// The auto-incrementing happens automatically in the storage layer for conditional updates.
	InstanceVersion int64 `json:"instanceVersion"`

	// PartitionKey is the storage shard/partition key for the document, it must be set before creation and must be all lowercase.
	// On the read-path, during our migration we will fill in an empty value based on the type we're reading.
	// Every type that embeds this struct must comment about what the PartitionKey is. For instance, subscriptionID, managementClusterID, etc.
	PartitionKey string `json:"partitionKey"`
}

var (
	_ DocumentPersistable      = &DocumentMetadata{}
	_ DocumentMetadataAccessor = &DocumentMetadata{}

	// documentIDUUIDNamespace was randomly created once.
	documentIDUUIDNamespace uuid.UUID
)

func init() {
	documentIDUUIDNamespace = Must(uuid.Parse("bf1ee0a1-0147-41ed-a083-d3cbbf7bea99"))
}

// DocumentPersistable is implemented by any type whose DocumentMetadata can be
// retrieved. Backend implementations use this to access storage metadata.
type DocumentPersistable interface {
	GetDocumentMetadata() *DocumentMetadata
}

// CosmosPersistable is a backward-compatible alias for DocumentPersistable.
// Existing code using GetCosmosData() continues to compile unchanged.
type CosmosPersistable = DocumentPersistable

func (o *DocumentMetadata) GetDocumentID() string {
	return Must(ResourceIDToDocumentID(o.ResourceID))
}

// GetCosmosUID is a backward-compatible alias for GetDocumentID.
func (o *DocumentMetadata) GetCosmosUID() string {
	return o.GetDocumentID()
}

// GetPartitionKey returns the lowercased shard key stored on the metadata.
// Kept for backward compatibility — new code should use GetShardKey.
func (o *DocumentMetadata) GetPartitionKey() string {
	return o.GetShardKey()
}

// GetShardKey returns the lowercased shard/partition key stored on the
// metadata. The storage backend is responsible for populating this field
// on the write path and on the read path; callers may rely on it being set
// after a successful Create/Get round-trip.
func (o *DocumentMetadata) GetShardKey() string {
	return strings.ToLower(o.PartitionKey)
}

// SetPartitionKey stores the shard key. Kept for backward compatibility.
func (o *DocumentMetadata) SetPartitionKey(partitionKey string) {
	o.SetShardKey(partitionKey)
}

// SetShardKey stores the shard/partition key on the metadata, lowercasing
// the supplied value.
func (o *DocumentMetadata) SetShardKey(shardKey string) {
	o.PartitionKey = strings.ToLower(shardKey)
}

func (o *DocumentMetadata) GetResourceID() *azcorearm.ResourceID {
	return o.ResourceID
}

func (o *DocumentMetadata) SetResourceID(resourceID *azcorearm.ResourceID) {
	o.ResourceID = resourceID
}

func (o *DocumentMetadata) GetEtag() azcore.ETag {
	return o.CosmosETag
}

func (o *DocumentMetadata) SetEtag(etag azcore.ETag) {
	o.CosmosETag = etag
}

// GetInstanceVersion returns the monotonically-increasing version counter
// stored on the document. The storage layer auto-increments it via SetInstanceVersion
// on every Replace (see PrepareForReplace).
func (o *DocumentMetadata) GetInstanceVersion() int64 {
	return o.InstanceVersion
}

// SetInstanceVersion overwrites the version counter. The storage layer is the
// only legitimate caller; tests can read it via GetInstanceVersion to assert
// the increment happened.
func (o *DocumentMetadata) SetInstanceVersion(v int64) {
	o.InstanceVersion = v
}

func (o *DocumentMetadata) GetDocumentMetadata() *DocumentMetadata {
	return o
}

// GetCosmosData is a backward-compatible alias for GetDocumentMetadata.
func (o *DocumentMetadata) GetCosmosData() *DocumentMetadata {
	return o.GetDocumentMetadata()
}

// DocumentMetadataAccessor is the full interface that all persistable types must implement.
type DocumentMetadataAccessor interface {
	DocumentPersistable
	GetDocumentID() string
	GetCosmosUID() string // backward-compat alias
	GetResourceID() *azcorearm.ResourceID
	SetResourceID(*azcorearm.ResourceID)
	GetEtag() azcore.ETag
	SetEtag(etag azcore.ETag)
	GetShardKey() string
	GetPartitionKey() string // backward-compat alias
	SetShardKey(string)
	SetPartitionKey(string) // backward-compat alias
	GetInstanceVersion() int64
	SetInstanceVersion(int64)
}

// CosmosMetadataAccessor is a backward-compatible alias for DocumentMetadataAccessor.
type CosmosMetadataAccessor = DocumentMetadataAccessor

// DocumentMetadataAccessorPtr constrains a type parameter to be a pointer to T
// that also implements DocumentMetadataAccessor. Generic CRUD code uses this so
// that a `*T` newObj argument is guaranteed to expose the metadata accessors
// at compile time, without runtime type assertions.
type DocumentMetadataAccessorPtr[T any] interface {
	*T
	DocumentMetadataAccessor
}

// CosmosMetadataAccessorPtr is a backward-compatible alias for DocumentMetadataAccessorPtr.
type CosmosMetadataAccessorPtr[T any] = DocumentMetadataAccessorPtr[T]

// CosmosMetadata is a backward-compatible alias for DocumentMetadata.
// Existing code embedding CosmosMetadata continues to compile unchanged.
type CosmosMetadata = DocumentMetadata

// ResourceIDToDocumentID derives a stable UUID document ID from a resource ID.
func ResourceIDToDocumentID(resourceID *azcorearm.ResourceID) (string, error) {
	if resourceID == nil {
		return "", errors.New("resource ID is nil")
	}
	return ResourceIDStringToDocumentID(resourceID.String())
}

// ResourceIDToCosmosID is a backward-compatible alias for ResourceIDToDocumentID.
func ResourceIDToCosmosID(resourceID *azcorearm.ResourceID) (string, error) {
	return ResourceIDToDocumentID(resourceID)
}

// ResourceIDStringToDocumentID derives a stable UUID document ID from a resource ID string.
func ResourceIDStringToDocumentID(resourceID string) (string, error) {
	if len(resourceID) == 0 {
		return "", errors.New("resource ID is empty")
	}

	// we predictably hash the values because there are length limitations on Azure.
	return uuid.NewSHA1(documentIDUUIDNamespace, []byte(strings.ToLower(resourceID))).String(), nil
}

// ResourceIDStringToCosmosID is a backward-compatible alias for ResourceIDStringToDocumentID.
func ResourceIDStringToCosmosID(resourceID string) (string, error) {
	return ResourceIDStringToDocumentID(resourceID)
}

// DeepCopyResourceID creates a true deep copy of an azcorearm.ResourceID by
// round-tripping through its string representation. This is necessary because
// ResourceID contains unexported fields (including parent pointers) that cannot
// be copied by simple struct assignment.
func DeepCopyResourceID(id *azcorearm.ResourceID) *azcorearm.ResourceID {
	if id == nil {
		return nil
	}
	resourceIDString := id.String()
	if len(resourceIDString) == 0 { // weird edge case.
		return &azcorearm.ResourceID{}
	}

	copied, err := azcorearm.ParseResourceID(resourceIDString)
	if err != nil {
		panic(fmt.Sprintf("failed to deep copy ResourceID %q: %v", id.String(), err))
	}
	return copied
}

// Must is a helper function that takes a value and error, returns the value if no error occurred,
// or panics if an error occurred. This is useful for test setup where we don't expect errors.
func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
