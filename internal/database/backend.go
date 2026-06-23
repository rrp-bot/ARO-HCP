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

// Package database defines the StorageBackend interface that decouples the
// kube-applier's CRUD and lister machinery from any specific database
// technology.  The Cosmos DB implementation (backends/cosmos) and the
// DynamoDB implementation (backends/dynamodb) both satisfy this interface;
// the concrete backend is selected at startup via the --backend flag.

package database

import "context"

// QueryOptions controls pagination for StorageBackend.Query.
type QueryOptions struct {
	// PageSizeHint is a hint for how many items to return per page.
	// A non-positive value lets the backend choose.
	PageSizeHint int32
	// ContinuationToken resumes a previous query from where it left off.
	// Empty string starts from the beginning.
	ContinuationToken string
}

// QueryResult is returned by StorageBackend.Query.
type QueryResult struct {
	// Items contains the raw JSON blobs for each matching document.
	Items [][]byte
	// ContinuationToken is non-empty when more pages are available.
	ContinuationToken string
}

// StorageBackend is the narrow persistence interface that all database
// implementations must satisfy.  Each method works on a single document
// identified by (documentID, shardKey):
//
//   - documentID  – stable UUID derived from the resource ID
//     (see arm.ResourceIDToDocumentID).
//   - shardKey    – lowercased management-cluster resource ID used for
//     partitioning; maps to the Cosmos partition key or DynamoDB
//     hash key.
//
// Implementations must be safe for concurrent use.
type StorageBackend interface {
	// Get returns the raw JSON blob and current etag for a document.
	// Returns (nil, "", ErrNotFound) when the document does not exist.
	Get(ctx context.Context, documentID, shardKey string) (data []byte, etag string, err error)

	// Create writes a new document and returns the stored JSON and the
	// backend-assigned etag.  Fails if a document with the same
	// documentID already exists in the shard.
	Create(ctx context.Context, documentID, shardKey string, data []byte) (stored []byte, etag string, err error)

	// Replace overwrites an existing document using an optimistic
	// concurrency check on etag.  Returns ErrPreconditionFailed when
	// the stored etag no longer matches.  Returns the updated JSON and
	// new etag on success.
	Replace(ctx context.Context, documentID, shardKey, etag string, data []byte) (stored []byte, newEtag string, err error)

	// Delete removes a document.  If the document does not exist the
	// call succeeds silently (idempotent).
	Delete(ctx context.Context, documentID, shardKey string) error

	// Query returns all documents in shardKey whose resourceType
	// attribute matches any value in resourceTypes.  Pass opts == nil
	// to use backend defaults (all pages, no continuation).
	Query(ctx context.Context, shardKey string, resourceTypes []string, opts *QueryOptions) (QueryResult, error)
}

// ErrNotFound is returned by StorageBackend.Get when no document exists.
type ErrNotFound struct{ DocumentID string }

func (e ErrNotFound) Error() string { return "document not found: " + e.DocumentID }

// ErrPreconditionFailed is returned by StorageBackend.Replace when the
// stored etag does not match the caller's expected etag.
type ErrPreconditionFailed struct {
	DocumentID   string
	StoredEtag   string
	ExpectedEtag string
}

func (e ErrPreconditionFailed) Error() string {
	return "precondition failed for document " + e.DocumentID +
		": stored etag " + e.StoredEtag + " != expected " + e.ExpectedEtag
}
