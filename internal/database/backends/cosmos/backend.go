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

// Package cosmos implements database.StorageBackend against Azure Cosmos DB.
// This is a thin extraction of the original per-container CRUD helpers from
// internal/database/crud_helpers.go, adapted to satisfy the backend-neutral
// StorageBackend interface so the kube-applier can be wired with either Cosmos
// or another backend (e.g. DynamoDB) at startup via --backend flag.
package cosmos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/Azure/ARO-HCP/internal/database"
)

// Backend implements database.StorageBackend against a single Cosmos DB container.
// The container holds one management cluster's *Desire documents; shardKey
// (the lowercased management-cluster resource ID) is used as the Cosmos partition key.
type Backend struct {
	container *azcosmos.ContainerClient
}

var _ database.StorageBackend = &Backend{}

// New wraps an already-opened Cosmos container client.
func New(container *azcosmos.ContainerClient) *Backend {
	return &Backend{container: container}
}

// document is the minimal envelope needed to read/write the resourceType field
// used by Query without deserialising the full internal type.
type document struct {
	ID           string      `json:"id"`
	ResourceType string      `json:"resourceType"`
	PartitionKey string      `json:"partitionKey"`
	CosmosETag   azcore.ETag `json:"_etag,omitempty"`
}

// Get returns the raw JSON and current etag for a document.
func (b *Backend) Get(ctx context.Context, documentID, shardKey string) ([]byte, string, error) {
	resp, err := b.container.ReadItem(
		ctx,
		azcosmos.NewPartitionKeyString(strings.ToLower(shardKey)),
		strings.ToLower(documentID),
		nil,
	)
	if err != nil {
		if isNotFound(err) {
			return nil, "", database.ErrNotFound{DocumentID: documentID}
		}
		return nil, "", fmt.Errorf("cosmos Get %s: %w", documentID, err)
	}
	etag := string(resp.ETag)
	return resp.Value, etag, nil
}

// Create writes a new document. Fails with a conflict error if the document
// already exists (Cosmos 409).
func (b *Backend) Create(ctx context.Context, documentID, shardKey string, data []byte) ([]byte, string, error) {
	resp, err := b.container.CreateItem(
		ctx,
		azcosmos.NewPartitionKeyString(strings.ToLower(shardKey)),
		data,
		&azcosmos.ItemOptions{EnableContentResponseOnWrite: true},
	)
	if err != nil {
		return nil, "", fmt.Errorf("cosmos Create %s: %w", documentID, err)
	}
	return resp.Value, string(resp.ETag), nil
}

// Replace overwrites an existing document with an optimistic concurrency check.
func (b *Backend) Replace(ctx context.Context, documentID, shardKey, etag string, data []byte) ([]byte, string, error) {
	etagVal := azcore.ETag(etag)
	resp, err := b.container.ReplaceItem(
		ctx,
		azcosmos.NewPartitionKeyString(strings.ToLower(shardKey)),
		strings.ToLower(documentID),
		data,
		&azcosmos.ItemOptions{
			IfMatchEtag:                  &etagVal,
			EnableContentResponseOnWrite: true,
		},
	)
	if err != nil {
		if isPreconditionFailed(err) {
			return nil, "", database.ErrPreconditionFailed{
				DocumentID:   documentID,
				ExpectedEtag: etag,
			}
		}
		return nil, "", fmt.Errorf("cosmos Replace %s: %w", documentID, err)
	}
	return resp.Value, string(resp.ETag), nil
}

// Delete removes a document. Idempotent — succeeds silently if absent.
func (b *Backend) Delete(ctx context.Context, documentID, shardKey string) error {
	_, err := b.container.DeleteItem(
		ctx,
		azcosmos.NewPartitionKeyString(strings.ToLower(shardKey)),
		strings.ToLower(documentID),
		nil,
	)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("cosmos Delete %s: %w", documentID, err)
	}
	return nil
}

// Query returns all documents in shardKey whose resourceType matches one of
// the supplied types. Pass opts == nil for defaults (all pages).
func (b *Backend) Query(ctx context.Context, shardKey string, resourceTypes []string, opts *database.QueryOptions) (database.QueryResult, error) {
	if len(resourceTypes) == 0 {
		return database.QueryResult{}, nil
	}

	// Build WHERE clause: STRINGEQUALS(c.resourceType, @t0, true) OR ...
	conditions := make([]string, len(resourceTypes))
	params := make([]azcosmos.QueryParameter, len(resourceTypes))
	for i, rt := range resourceTypes {
		name := fmt.Sprintf("@rt%d", i)
		conditions[i] = fmt.Sprintf("STRINGEQUALS(c.resourceType, %s, true)", name)
		params[i] = azcosmos.QueryParameter{Name: name, Value: rt}
	}
	query := "SELECT * FROM c WHERE LENGTH(c.resourceID) > 0 AND (" +
		strings.Join(conditions, " OR ") + ")"

	qopts := azcosmos.QueryOptions{
		PageSizeHint:    -1,
		QueryParameters: params,
	}
	if opts != nil {
		if opts.PageSizeHint > 0 {
			qopts.PageSizeHint = opts.PageSizeHint
		}
		if opts.ContinuationToken != "" {
			qopts.ContinuationToken = &opts.ContinuationToken
		}
	}

	pk := azcosmos.NewPartitionKeyString(strings.ToLower(shardKey))
	pager := b.container.NewQueryItemsPager(query, pk, &qopts)

	var result database.QueryResult
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return database.QueryResult{}, fmt.Errorf("cosmos Query: %w", err)
		}
		for _, item := range page.Items {
			cp := make([]byte, len(item))
			copy(cp, item)
			result.Items = append(result.Items, cp)
		}
		if opts != nil && opts.PageSizeHint > 0 {
			// Single-page mode: stop after first page and capture token.
			if page.ContinuationToken != nil {
				result.ContinuationToken = *page.ContinuationToken
			}
			break
		}
	}
	return result, nil
}

// isNotFound returns true for Cosmos 404 responses.
func isNotFound(err error) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == 404
}

// isPreconditionFailed returns true for Cosmos 412 responses.
func isPreconditionFailed(err error) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == 412
}

// EtagFromJSON extracts the Cosmos-assigned _etag from a raw JSON document blob.
// Used by the kube-applier client layer to round-trip etags from stored bytes.
func EtagFromJSON(data []byte) (string, error) {
	var d struct {
		ETag string `json:"_etag"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return "", err
	}
	return d.ETag, nil
}
