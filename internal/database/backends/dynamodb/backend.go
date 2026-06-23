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

// Package dynamodb implements database.StorageBackend against AWS DynamoDB.
//
// # Table design
//
// A single DynamoDB table is used with the following key schema:
//
//	Partition key (HASH):  shardKey      – string, lowercased MC resource ID
//	Sort key (RANGE):      documentID    – string, UUID derived from resource ID
//
// Additional attributes stored per item:
//
//	resourceType – string, used for filtered list queries
//	etag         – string, UUID v4 generated on every write, used for
//	               optimistic-concurrency conditional expressions
//	data         – string (JSON), the full serialised *Desire document
//
// A Global Secondary Index (GSI) named "resourceType-index" projects all
// attributes and has:
//
//	GSI partition key: shardKey
//	GSI sort key:      resourceType
//
// This lets Query filter by (shardKey, resourceType) efficiently without a
// full-table scan.
//
// # Optimistic concurrency
//
// Create uses attribute_not_exists(documentID) to prevent overwriting an
// existing item.  Replace uses a ConditionExpression of "etag = :expected"
// and maps ConditionalCheckFailedException to ErrPreconditionFailed so the
// kube-applier's desirestatuswriter can handle it exactly as it handles a
// Cosmos 412.
package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	database "github.com/Azure/ARO-HCP/internal/database"
)

const (
	// AttributeShardKey is the DynamoDB partition-key attribute name.
	AttributeShardKey = "shardKey"
	// AttributeDocumentID is the DynamoDB sort-key attribute name.
	AttributeDocumentID = "documentID"
	// AttributeResourceType is indexed by the GSI for filtered queries.
	AttributeResourceType = "resourceType"
	// AttributeEtag holds the optimistic-concurrency token.
	AttributeEtag = "etag"
	// AttributeData holds the full serialised JSON document.
	AttributeData = "data"

	// ResourceTypeGSI is the name of the Global Secondary Index used by Query.
	ResourceTypeGSI = "resourceType-index"
)

// item is the full DynamoDB record shape.
type item struct {
	ShardKey     string `dynamodbav:"shardKey"`
	DocumentID   string `dynamodbav:"documentID"`
	ResourceType string `dynamodbav:"resourceType"`
	Etag         string `dynamodbav:"etag"`
	Data         string `dynamodbav:"data"` // raw JSON blob
}

// resourceTypeDoc is a minimal struct for extracting resourceType from a
// JSON document blob without deserialising the entire payload.
type resourceTypeDoc struct {
	ResourceType string `json:"resourceType"`
}

// Backend implements database.StorageBackend against AWS DynamoDB.
type Backend struct {
	client    *dynamodb.Client
	tableName string
}

var _ database.StorageBackend = &Backend{}

// New constructs a Backend using the provided DynamoDB client and table name.
// The table must already exist with the key schema described in the package
// comment.  Use NewFromConfig to build the client from an aws.Config.
func New(client *dynamodb.Client, tableName string) *Backend {
	return &Backend{client: client, tableName: tableName}
}

// NewFromConfig builds a Backend directly from an aws.Config.
func NewFromConfig(cfg aws.Config, tableName string) *Backend {
	return New(dynamodb.NewFromConfig(cfg), tableName)
}

// Get returns the raw JSON data and current etag for a document.
// Returns ErrNotFound if the item does not exist.
func (b *Backend) Get(ctx context.Context, documentID, shardKey string) ([]byte, string, error) {
	out, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(b.tableName),
		ConsistentRead: aws.Bool(true),
		Key:            b.key(documentID, shardKey),
	})
	if err != nil {
		return nil, "", fmt.Errorf("dynamodb Get %s: %w", documentID, err)
	}
	if len(out.Item) == 0 {
		return nil, "", database.ErrNotFound{DocumentID: documentID}
	}

	var it item
	if err := attributevalue.UnmarshalMap(out.Item, &it); err != nil {
		return nil, "", fmt.Errorf("dynamodb Get %s unmarshal: %w", documentID, err)
	}
	return []byte(it.Data), it.Etag, nil
}

// Create writes a new document, failing if one already exists.
func (b *Backend) Create(ctx context.Context, documentID, shardKey string, data []byte) ([]byte, string, error) {
	etag := uuid.NewString()
	rt, err := extractResourceType(data)
	if err != nil {
		return nil, "", fmt.Errorf("dynamodb Create %s extractResourceType: %w", documentID, err)
	}

	it := item{
		ShardKey:     strings.ToLower(shardKey),
		DocumentID:   strings.ToLower(documentID),
		ResourceType: rt,
		Etag:         etag,
		Data:         string(data),
	}
	av, err := attributevalue.MarshalMap(it)
	if err != nil {
		return nil, "", fmt.Errorf("dynamodb Create %s marshal: %w", documentID, err)
	}

	_, err = b.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(b.tableName),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(#dk)"),
		ExpressionAttributeNames: map[string]string{
			"#dk": AttributeDocumentID,
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return nil, "", fmt.Errorf("dynamodb Create %s: item already exists", documentID)
		}
		return nil, "", fmt.Errorf("dynamodb Create %s: %w", documentID, err)
	}

	// Embed the assigned etag into the returned data so callers can round-trip it.
	stored, err := embedEtag(data, etag)
	if err != nil {
		return nil, "", fmt.Errorf("dynamodb Create %s embedEtag: %w", documentID, err)
	}
	return stored, etag, nil
}

// Replace overwrites an existing document using an optimistic concurrency
// check on the etag.  Returns ErrPreconditionFailed on etag mismatch.
func (b *Backend) Replace(ctx context.Context, documentID, shardKey, etag string, data []byte) ([]byte, string, error) {
	newEtag := uuid.NewString()
	rt, err := extractResourceType(data)
	if err != nil {
		return nil, "", fmt.Errorf("dynamodb Replace %s extractResourceType: %w", documentID, err)
	}

	it := item{
		ShardKey:     strings.ToLower(shardKey),
		DocumentID:   strings.ToLower(documentID),
		ResourceType: rt,
		Etag:         newEtag,
		Data:         string(data),
	}
	av, err := attributevalue.MarshalMap(it)
	if err != nil {
		return nil, "", fmt.Errorf("dynamodb Replace %s marshal: %w", documentID, err)
	}

	_, err = b.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(b.tableName),
		Item:                av,
		ConditionExpression: aws.String("#etag = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#etag": AttributeEtag,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expected": &types.AttributeValueMemberS{Value: etag},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return nil, "", database.ErrPreconditionFailed{
				DocumentID:   documentID,
				ExpectedEtag: etag,
			}
		}
		return nil, "", fmt.Errorf("dynamodb Replace %s: %w", documentID, err)
	}

	stored, err := embedEtag(data, newEtag)
	if err != nil {
		return nil, "", fmt.Errorf("dynamodb Replace %s embedEtag: %w", documentID, err)
	}
	return stored, newEtag, nil
}

// Delete removes a document. Idempotent — succeeds silently if absent.
func (b *Backend) Delete(ctx context.Context, documentID, shardKey string) error {
	_, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(b.tableName),
		Key:       b.key(documentID, shardKey),
	})
	if err != nil {
		return fmt.Errorf("dynamodb Delete %s: %w", documentID, err)
	}
	return nil
}

// Query returns all documents for shardKey whose resourceType matches any of
// the supplied types.  Uses the resourceType-index GSI.
// Opts == nil returns all pages.
func (b *Backend) Query(ctx context.Context, shardKey string, resourceTypes []string, opts *database.QueryOptions) (database.QueryResult, error) {
	if len(resourceTypes) == 0 {
		return database.QueryResult{}, nil
	}

	// Build a filter expression: resourceType IN (:rt0, :rt1, ...)
	// DynamoDB doesn't support IN on key conditions in a GSI query when the
	// sort key is resourceType, so we query by shardKey only and filter.
	// For small fleets (low thousands of items per shard) this is fine.
	filterParts := make([]string, len(resourceTypes))
	exprValues := map[string]types.AttributeValue{
		":sk": &types.AttributeValueMemberS{Value: strings.ToLower(shardKey)},
	}
	for i, rt := range resourceTypes {
		placeholder := fmt.Sprintf(":rt%d", i)
		filterParts[i] = fmt.Sprintf("resourceType = %s", placeholder)
		exprValues[placeholder] = &types.AttributeValueMemberS{Value: rt}
	}
	filterExpr := strings.Join(filterParts, " OR ")

	input := &dynamodb.QueryInput{
		TableName:              aws.String(b.tableName),
		KeyConditionExpression: aws.String("shardKey = :sk"),
		FilterExpression:       aws.String(filterExpr),
		ExpressionAttributeValues: exprValues,
		ConsistentRead:         aws.Bool(true),
	}

	if opts != nil && opts.PageSizeHint > 0 {
		input.Limit = aws.Int32(opts.PageSizeHint)
	}
	if opts != nil && opts.ContinuationToken != "" {
		startKey, err := decodeContinuationToken(opts.ContinuationToken)
		if err != nil {
			return database.QueryResult{}, fmt.Errorf("dynamodb Query decode continuation token: %w", err)
		}
		input.ExclusiveStartKey = startKey
	}

	var result database.QueryResult
	paginator := dynamodb.NewQueryPaginator(b.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return database.QueryResult{}, fmt.Errorf("dynamodb Query: %w", err)
		}
		for _, av := range page.Items {
			var it item
			if err := attributevalue.UnmarshalMap(av, &it); err != nil {
				return database.QueryResult{}, fmt.Errorf("dynamodb Query unmarshal: %w", err)
			}
			data, err := embedEtag([]byte(it.Data), it.Etag)
			if err != nil {
				return database.QueryResult{}, fmt.Errorf("dynamodb Query embedEtag: %w", err)
			}
			result.Items = append(result.Items, data)
		}
		if opts != nil && opts.PageSizeHint > 0 {
			// Single-page mode.
			if page.LastEvaluatedKey != nil {
				token, err := encodeContinuationToken(page.LastEvaluatedKey)
				if err != nil {
					return database.QueryResult{}, fmt.Errorf("dynamodb Query encode continuation token: %w", err)
				}
				result.ContinuationToken = token
			}
			break
		}
	}
	return result, nil
}

// key builds the DynamoDB primary key map for a given documentID and shardKey.
func (b *Backend) key(documentID, shardKey string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		AttributeShardKey:   &types.AttributeValueMemberS{Value: strings.ToLower(shardKey)},
		AttributeDocumentID: &types.AttributeValueMemberS{Value: strings.ToLower(documentID)},
	}
}

// isConditionalCheckFailed returns true when DynamoDB rejected the write
// because the ConditionExpression was not satisfied.
func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

// extractResourceType reads the resourceType field from a serialised JSON document.
func extractResourceType(data []byte) (string, error) {
	var d resourceTypeDoc
	if err := json.Unmarshal(data, &d); err != nil {
		return "", err
	}
	return d.ResourceType, nil
}

// embedEtag injects or overwrites the "_etag" field in a JSON document blob
// so the returned bytes look identical to what the Cosmos backend returns
// (which carries _etag inside the document body). This allows the upper
// CRUD layer to be backend-agnostic when round-tripping etags.
func embedEtag(data []byte, etag string) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	etagJSON, err := json.Marshal(etag)
	if err != nil {
		return nil, err
	}
	m["_etag"] = json.RawMessage(etagJSON)
	return json.Marshal(m)
}

// encodeContinuationToken serialises a DynamoDB LastEvaluatedKey to a
// base64-safe JSON string for use as a continuation token in QueryResult.
func encodeContinuationToken(key map[string]types.AttributeValue) (string, error) {
	// Marshal to a simple string-keyed map.
	simple := make(map[string]string)
	for k, v := range key {
		if s, ok := v.(*types.AttributeValueMemberS); ok {
			simple[k] = s.Value
		}
	}
	b, err := json.Marshal(simple)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeContinuationToken is the inverse of encodeContinuationToken.
func decodeContinuationToken(token string) (map[string]types.AttributeValue, error) {
	var simple map[string]string
	if err := json.Unmarshal([]byte(token), &simple); err != nil {
		return nil, err
	}
	out := make(map[string]types.AttributeValue, len(simple))
	for k, v := range simple {
		out[k] = &types.AttributeValueMemberS{Value: v}
	}
	return out, nil
}
