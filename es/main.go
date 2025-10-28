package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"log"
	"os"

	"github.com/elastic/go-elasticsearch/v8"
)

func getMappings(index string) string {

	return `
{
    "mappings": {
        "dynamic_templates": [
            {
                "attributes_specific_fields": {
                    "path_match": "attributes.*",
                    "mapping": {
                        "type": "flattened"
                    }
                }
            }  
        ],
        "properties": {
            "resource_id": {
                "type": "keyword"
            },
            "attributes": {
                "properties": {
                    "location": {
                        "type": "keyword"
                    }
                }
            }
        }
    },
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "mapping": {
                "total_fields": {
                    "limit": 20000
                }
            }
        }
    }
}
`

}

// ESClient Elasticsearch客户端封装
type ESClient struct {
	index  string
	client *elasticsearch.Client
}

// NewESClient 创建ES客户端
func NewESClient(url, index string) (*ESClient, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{url}})
	if err != nil {
		return nil, err
	}
	return &ESClient{
		index:  index,
		client: client,
	}, nil
}

// CreateIndex 创建索引
func (esc *ESClient) CreateIndex() error {
	mappings := getMappings(esc.index)
	req := esapi.IndicesCreateRequest{
		Index: esc.index,
		Body:  bytes.NewReader([]byte(mappings)),
	}

	res, err := req.Do(context.Background(), esc.client)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		if res.StatusCode == 400 {
			log.Printf("索引 %s 已经存在\n", esc.index)
			return nil
		}
		return fmt.Errorf("创建索引失败 %s", res.String())
	}

	fmt.Printf("索引 %s 创建成功\n", esc.index)
	return nil
}

// InsertProjectRoot 插入数据
func (esc *ESClient) InsertProjectRoot(data map[string]interface{}) error {
	ctx := context.Background()

	id := data["_id"].(string)
	delete(data, "_id")
	marshal, _ := json.Marshal(data)

	req := esapi.IndexRequest{
		Index:      esc.index,
		DocumentID: id,
		Body:       bytes.NewReader(marshal),
	}
	res, err := req.Do(ctx, esc.client)

	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Printf("插入数据失败 %s: %s", id, res.String())
		return fmt.Errorf("插入数据失败")
	}

	return nil
}

// 示例使用
func main() {
	var data map[string]interface{}
	file, err := os.ReadFile("D:\\code\\mock-go\\es\\33_158.json")
	if err != nil {
		return
	}
	err = json.Unmarshal(file, &data)
	if err != nil {
		return
	}

	resourcesI := data["resources"]

	client, err := NewESClient("http://127.0.0.1:9200", "resources")
	if err != nil {
		return
	}
	err = client.CreateIndex()
	if err != nil {
		fmt.Println("CreateIndex", err)
		return
	}
	for i := range resourcesI.([]interface{}) {
		resource := resourcesI.([]interface{})[i].(map[string]interface{})
		id := resource["_id"].(string)
		err = client.InsertProjectRoot(resource)
		if err != nil {
			fmt.Println("InsertProjectRoot", id, err)
			return
		}
	}

}
