package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/debug"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type Resource struct {
	ResourceId string                 `json:"resource_id" bson:"resource_id"`
	ParentId   string                 `json:"parent_id" bson:"parent_id"`
	Version    int                    `json:"version" bson:"version"`
	Deleted    int                    `json:"deleted" bson:"deleted"`
	Attributes map[string]interface{} `json:"attributes" bson:"attributes"`
}

func must(err error) {
	if err != nil {
		debug.PrintStack()
		log.Fatalf("error: %v", err)

	}
}

func newESClient() *elastic.Client {
	cfg := elastic.Config{
		Addresses: []string{getEnv("ES_URL", "http://localhost:9200")},
	}
	if u := os.Getenv("ES_USERNAME"); u != "" {
		cfg.Username = u
	}
	if p := os.Getenv("ES_PASSWORD"); p != "" {
		cfg.Password = p
	}
	es, err := elastic.NewClient(cfg)
	must(err)
	// ping
	_, err = es.Info()
	must(err)
	return es
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// generateLargeAttributes builds a multi-level map with large strings to reach approx targetBytes
func generateLargeAttributes(targetBytes int) map[string]interface{} {
	root := make(map[string]interface{})
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// helper to create a random string of length n
	randStr := func(n int) string {
		letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		b := make([]byte, n)
		for i := range b {
			b[i] = letters[rnd.Intn(len(letters))]
		}
		return string(b)
	}

	// create many nested entries
	total := 0
	idx := 0
	for total < targetBytes {
		// create a nested map with several fields
		level1 := fmt.Sprintf("node_%04d", idx)
		nm := make(map[string]interface{})
		nm["meta"] = map[string]interface{}{
			"title":       fmt.Sprintf("Title %d", idx),
			"description": randStr(1024), // 1KB
			"tags":        []string{"big", "test", fmt.Sprintf("idx_%d", idx)},
		}
		// add a deep nested object
		deep := make(map[string]interface{})
		for j := 0; j < 3; j++ {
			deep[fmt.Sprintf("deep_%d", j)] = map[string]interface{}{
				"text": randStr(2048), // 2KB each
				"num":  j,
			}
		}
		nm["deep"] = deep

		// add a large blob-like string to increase size
		blobSize := 16*1024 + rnd.Intn(16*1024) // 16KB ~ 32KB
		nm["blob"] = randStr(blobSize)

		root[level1] = nm

		total += len(level1) + 1024 + 3*(2048+10) + blobSize
		idx++
		// safety upper bound
		if idx > 2000 {
			break
		}
	}
	// add some numeric fields at root for queries/aggregations
	root["price"] = 12345.67
	root["count"] = 999
	root["status"] = "active"
	return root
}

func ensureIndex(es *elastic.Client, index string) {

	// delete old index if exists (for testing convenience)
	es.Indices.Delete([]string{index})

	settings := map[string]interface{}{
		"settings": map[string]interface{}{
			"index.mapping.total_fields.limit": 20000,
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"resource_id": map[string]interface{}{"type": "keyword"},
				"parent_id":   map[string]interface{}{"type": "keyword"},
				"version":     map[string]interface{}{"type": "integer"},
				"deleted":     map[string]interface{}{"type": "integer"},
				"attributes": map[string]interface{}{
					"type":    "object",
					"dynamic": true, // 允许自动生成子字段
				},
			},
		},
	}

	body, _ := json.Marshal(settings)
	res, err := es.Indices.Create(index, es.Indices.Create.WithBody(bytes.NewReader(body)))
	must(err)
	defer res.Body.Close()
	fmt.Println("index created with high field limit (20000)")
}

func indexDocument(es *elastic.Client, index string, id string, doc interface{}) {
	ctx := context.Background()
	b, err := json.Marshal(doc)
	must(err)
	fmt.Printf("document JSON size: %.2f MB\n", float64(len(b))/1024.0/1024.0)

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: id,
		Body:       bytes.NewReader(b),
		Refresh:    "true", // for immediate searchability in tests
	}
	start := time.Now()
	res, err := req.Do(ctx, es)
	must(err)
	defer res.Body.Close()
	fmt.Printf("index response status: %s, elapsed: %v\n", res.Status(), time.Since(start))
	if res.IsError() {
		log.Fatalf("indexing failed: %s", res.String())
	}
}

func getByID(es *elastic.Client, index string, id string) {
	start := time.Now()
	res, err := es.Get(index, id)
	must(err)
	defer res.Body.Close()
	fmt.Printf("GET by ID status: %s, elapsed: %v\n", res.Status(), time.Since(start))
	if res.IsError() {
		fmt.Println("get error or not found")
		return
	}
	var got map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
		fmt.Println("decode get response error:", err)
		return
	}
	// print small summary
	if src, ok := got["_source"]; ok {
		srcMap := src.(map[string]interface{})
		fmt.Printf("retrieved resource_id: %v, version: %v\n", srcMap["resource_id"], srcMap["version"])
	}
}

func searchMatchAttribute(es *elastic.Client, index string, fieldPath string, text string, size int) {
	// match query on a text field under attributes
	q := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				fieldPath: text,
			},
		},
	}
	runSearch(es, index, q, size, "match")
}

func searchTerm(es *elastic.Client, index string, field string, value interface{}, size int) {
	q := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				field: map[string]interface{}{"value": value},
			},
		},
	}
	runSearch(es, index, q, size, "term")
}

func searchExists(es *elastic.Client, index string, field string, size int) {
	q := map[string]interface{}{
		"query": map[string]interface{}{
			"exists": map[string]interface{}{
				"field": field,
			},
		},
	}
	runSearch(es, index, q, size, "exists")
}

func searchWildcard(es *elastic.Client, index string, field string, pattern string, size int) {
	q := map[string]interface{}{
		"query": map[string]interface{}{
			"wildcard": map[string]interface{}{
				field: map[string]interface{}{
					"value": pattern,
				},
			},
		},
	}
	runSearch(es, index, q, size, "wildcard")
}

func aggregationByVersion(es *elastic.Client, index string) {
	q := map[string]interface{}{
		"size": 0,
		"aggs": map[string]interface{}{
			"by_version": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "version",
				},
			},
		},
	}
	runSearch(es, index, q, 0, "agg")
}

func runSearch(es *elastic.Client, index string, q map[string]interface{}, size int, label string) {
	ctx := context.Background()
	if size > 0 {
		q["size"] = size
	}
	b, _ := json.Marshal(q)
	start := time.Now()
	res, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithBody(bytes.NewReader(b)),
	)
	must(err)
	defer res.Body.Close()
	fmt.Printf("[%s] search status: %s, elapsed: %v\n", label, res.Status(), time.Since(start))
	if res.IsError() {
		fmt.Printf("[%s] search error: %s\n", label, res.String())
		return
	}
	var out map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		fmt.Println("decode search response error:", err)
		return
	}
	// summarize hits
	hits := out["hits"].(map[string]interface{})
	total := hits["total"]
	fmt.Printf("[%s] total hits: %v\n", label, total)
	// print first hit id if exists
	if hitArr, ok := hits["hits"].([]interface{}); ok && len(hitArr) > 0 {
		hit0 := hitArr[0].(map[string]interface{})
		fmt.Printf("[%s] first hit _id: %v\n", label, hit0["_id"])
	}
	// if aggregations present, print keys
	if agg, ok := out["aggregations"]; ok {
		aggJSON, _ := json.MarshalIndent(agg, "", "  ")
		fmt.Printf("[%s] aggregations: %s\n", label, string(aggJSON))
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	es := newESClient()
	indexName := "resources_test"

	ensureIndex(es, indexName)

	// 1) generate resource with ~10MB attributes
	targetMB := 10
	targetBytes := targetMB * 1024 * 1024
	fmt.Printf("generating Attributes ~%d MB ...\n", targetMB)
	attrs := generateLargeAttributes(targetBytes)

	for i := 0; i < 10; i++ {
		attrs["resource_id"] = fmt.Sprintf("res-%d", i)
		resDoc := Resource{
			ResourceId: fmt.Sprintf("res-large-%3d", i),
			ParentId:   "parent-123",
			Version:    i,
			Deleted:    0,
			Attributes: attrs,
		}
		// index the document
		indexDocument(es, indexName, resDoc.ResourceId, resDoc)
	}

	// 2) get by id
	fmt.Println("GET by id ...")
	getByID(es, indexName, "res-large-001")

	// 3) match search (try to match on attributes.node_0000.meta.description)
	// note: if mapping doesn't tokenize that path, match may not find; it's a test
	fieldPath := "attributes.node_0000.meta.description"
	fmt.Println("search: match on a nested attributes field ...")
	searchMatchAttribute(es, indexName, fieldPath, "the", 5)

	// 4) term search on status (keyword)
	fmt.Println("search: term on attributes.status ...")
	searchTerm(es, indexName, "attributes.status.keyword", "active", 5)

	// 5) exists check
	fmt.Println("search: exists attributes.node_0000.blob ...")
	searchExists(es, indexName, "attributes.node_0000.blob", 5)

	// 6) wildcard search on some tag
	fmt.Println("search: wildcard on attributes.node_0000.meta.tags ...")
	// wildcard usually for keyword fields, try pattern
	searchWildcard(es, indexName, "attributes.node_0000.meta.tags.keyword", "idx_*", 5)

	// 7) aggregation by version
	fmt.Println("aggregation: by version ...")
	aggregationByVersion(es, indexName)

	searchTerm(es, indexName, "attributes.resource_id.keyword", "res-1", 0)

	fmt.Println("done.")
}
