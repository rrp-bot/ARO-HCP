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

// Package dynamodb_test contains integration tests for the DynamoDB
// StorageBackend.  They require a running LocalStack instance reachable at the
// URL in LOCALSTACK_ENDPOINT (e.g. http://localhost:4566).  When that env var
// is absent the tests are skipped.
package dynamodb_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	database "github.com/Azure/ARO-HCP/internal/database"
	dynbk "github.com/Azure/ARO-HCP/internal/database/backends/dynamodb"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// skipIfNoLocalStack skips the test when LOCALSTACK_ENDPOINT is unset.
func skipIfNoLocalStack(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("LOCALSTACK_ENDPOINT")
	if ep == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set — skipping LocalStack integration tests")
	}
	return ep
}

// newTestClient builds a DynamoDB client pointed at LocalStack.
func newTestClient(t *testing.T, endpoint string) *dynamodb.Client {
	t.Helper()
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
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

// createTable creates a DynamoDB table in LocalStack and registers a cleanup
// that deletes it when the test finishes.  The table uses shardKey (HASH) +
// documentID (RANGE) — matching the Backend's expected schema.
func createTable(t *testing.T, client *dynamodb.Client, tableName string) {
	t.Helper()
	ctx := context.Background()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
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

// newBackend creates the test table and returns a *dynbk.Backend wired to it.
func newBackend(t *testing.T, endpoint string) *dynbk.Backend {
	t.Helper()
	client := newTestClient(t, endpoint)
	tableName := fmt.Sprintf("test-table-%s", t.Name())
	// Sanitise table name: DynamoDB table names may only contain [a-zA-Z0-9_.-]
	sanitised := make([]byte, 0, len(tableName))
	for i := 0; i < len(tableName); i++ {
		ch := tableName[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			sanitised = append(sanitised, ch)
		} else {
			sanitised = append(sanitised, '-')
		}
	}
	tableName = string(sanitised)
	createTable(t, client, tableName)
	return dynbk.New(client, tableName)
}

// simpleDoc produces a minimal JSON document with the given resourceType.
func simpleDoc(resourceType string) []byte {
	b, _ := json.Marshal(map[string]string{"resourceType": resourceType, "hello": "world"})
	return b
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestBackend_GetNotFound(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	_, _, err := b.Get(ctx, "no-such-doc", "shard-1")
	if err == nil {
		t.Fatal("expected ErrNotFound, got nil")
	}
	var nf database.ErrNotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected ErrNotFound, got %T: %v", err, err)
	}
}

func TestBackend_CreateAndGet(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	data := simpleDoc("myResourceType")
	stored, etag1, err := b.Create(ctx, "doc-1", "shard-1", data)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if etag1 == "" {
		t.Fatal("Create returned empty etag")
	}

	// Validate _etag was embedded in returned blob.
	var m map[string]json.RawMessage
	if err := json.Unmarshal(stored, &m); err != nil {
		t.Fatalf("unmarshal stored: %v", err)
	}
	var embeddedEtag string
	if err := json.Unmarshal(m["_etag"], &embeddedEtag); err != nil {
		t.Fatalf("unmarshal _etag: %v", err)
	}
	if embeddedEtag != etag1 {
		t.Errorf("embedded etag %q != returned etag %q", embeddedEtag, etag1)
	}

	// Get should return the same content and etag.
	got, etag2, err := b.Get(ctx, "doc-1", "shard-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if etag2 != etag1 {
		t.Errorf("Get etag %q != Create etag %q", etag2, etag1)
	}
	_ = got
}

func TestBackend_CreateConflict(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	data := simpleDoc("myResourceType")
	if _, _, err := b.Create(ctx, "dup-doc", "shard-1", data); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	// Second Create for the same documentID must fail.
	_, _, err := b.Create(ctx, "dup-doc", "shard-1", data)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
}

func TestBackend_Replace(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	data := simpleDoc("myResourceType")
	_, etag1, err := b.Create(ctx, "rep-doc", "shard-1", data)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	updated := simpleDoc("myResourceType")
	_, etag2, err := b.Replace(ctx, "rep-doc", "shard-1", etag1, updated)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if etag2 == etag1 {
		t.Error("Replace should have assigned a new etag")
	}

	// Confirm the new etag is stored.
	_, etag3, err := b.Get(ctx, "rep-doc", "shard-1")
	if err != nil {
		t.Fatalf("Get after Replace: %v", err)
	}
	if etag3 != etag2 {
		t.Errorf("Get etag %q != Replace etag %q", etag3, etag2)
	}
}

func TestBackend_ReplacePreconditionFailed(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	data := simpleDoc("myResourceType")
	_, _, err := b.Create(ctx, "pf-doc", "shard-1", data)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Use a wrong etag — should get ErrPreconditionFailed.
	_, _, err = b.Replace(ctx, "pf-doc", "shard-1", "wrong-etag", data)
	if err == nil {
		t.Fatal("expected ErrPreconditionFailed, got nil")
	}
	var pf database.ErrPreconditionFailed
	if !errors.As(err, &pf) {
		t.Fatalf("expected ErrPreconditionFailed, got %T: %v", err, err)
	}
	if pf.DocumentID != "pf-doc" {
		t.Errorf("ErrPreconditionFailed.DocumentID = %q, want %q", pf.DocumentID, "pf-doc")
	}
}

func TestBackend_DeleteIdempotent(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	data := simpleDoc("myResourceType")
	if _, _, err := b.Create(ctx, "del-doc", "shard-1", data); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// First delete succeeds.
	if err := b.Delete(ctx, "del-doc", "shard-1"); err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	// Second delete (already gone) must also succeed — idempotent.
	if err := b.Delete(ctx, "del-doc", "shard-1"); err != nil {
		t.Fatalf("second Delete (idempotent): %v", err)
	}
	// Get after delete must return ErrNotFound.
	_, _, err := b.Get(ctx, "del-doc", "shard-1")
	var nf database.ErrNotFound
	if !errors.As(err, &nf) {
		t.Fatalf("expected ErrNotFound after Delete, got %T: %v", err, err)
	}
}

func TestBackend_Query(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	// Write three documents: two of type "typeA", one of type "typeB".
	for i, rt := range []string{"typeA", "typeA", "typeB"} {
		doc := simpleDoc(rt)
		if _, _, err := b.Create(ctx, fmt.Sprintf("q-doc-%d", i), "shard-q", doc); err != nil {
			t.Fatalf("Create doc %d: %v", i, err)
		}
	}

	// Query for typeA only — should return 2 items.
	result, err := b.Query(ctx, "shard-q", []string{"typeA"}, nil)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Items) != 2 {
		t.Errorf("Query typeA: got %d items, want 2", len(result.Items))
	}

	// Query for both types — should return 3 items.
	result2, err := b.Query(ctx, "shard-q", []string{"typeA", "typeB"}, nil)
	if err != nil {
		t.Fatalf("Query both types: %v", err)
	}
	if len(result2.Items) != 3 {
		t.Errorf("Query both: got %d items, want 3", len(result2.Items))
	}

	// Query for unknown type — should return 0 items.
	result3, err := b.Query(ctx, "shard-q", []string{"typeX"}, nil)
	if err != nil {
		t.Fatalf("Query unknown type: %v", err)
	}
	if len(result3.Items) != 0 {
		t.Errorf("Query unknown: got %d items, want 0", len(result3.Items))
	}
}

func TestBackend_QueryEmptyResourceTypes(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	// Querying with an empty resourceTypes slice must return an empty result
	// without hitting the backend.
	result, err := b.Query(ctx, "shard-q", nil, nil)
	if err != nil {
		t.Fatalf("Query nil types: %v", err)
	}
	if len(result.Items) != 0 {
		t.Errorf("expected 0 items for empty resourceTypes, got %d", len(result.Items))
	}
}

func TestBackend_QueryEmbedEtagInItems(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	data := simpleDoc("etag-check")
	_, etag, err := b.Create(ctx, "etag-doc", "shard-etag", data)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	result, err := b.Query(ctx, "shard-etag", []string{"etag-check"}, nil)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(result.Items))
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(result.Items[0], &m); err != nil {
		t.Fatalf("unmarshal query item: %v", err)
	}
	var gotEtag string
	if err := json.Unmarshal(m["_etag"], &gotEtag); err != nil {
		t.Fatalf("unmarshal _etag from query result: %v", err)
	}
	if gotEtag != etag {
		t.Errorf("query item _etag %q != stored etag %q", gotEtag, etag)
	}
}

func TestBackend_ConcurrentReplace(t *testing.T) {
	ep := skipIfNoLocalStack(t)
	b := newBackend(t, ep)
	ctx := context.Background()

	data := simpleDoc("concurrent")
	_, etag, err := b.Create(ctx, "conc-doc", "shard-conc", data)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Two concurrent replacers with the same etag: one should win, one should get ErrPreconditionFailed.
	type result struct {
		newEtag string
		err     error
	}
	ch := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, e, err := b.Replace(ctx, "conc-doc", "shard-conc", etag, simpleDoc("concurrent"))
			ch <- result{e, err}
		}()
	}
	r1 := <-ch
	r2 := <-ch

	wins := 0
	loses := 0
	for _, r := range []result{r1, r2} {
		if r.err == nil {
			wins++
		} else {
			var pf database.ErrPreconditionFailed
			if errors.As(r.err, &pf) {
				loses++
			} else {
				t.Errorf("unexpected error: %v", r.err)
			}
		}
	}
	if wins != 1 || loses != 1 {
		t.Errorf("concurrent replace: %d wins, %d loses — expected exactly 1 each", wins, loses)
	}
}
