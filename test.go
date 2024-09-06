package main

// import (
// 	"context"
// 	"fmt"
// 	"github.com/c12s/celestial/helper"
// 	"github.com/c12s/celestial/model"
// 	"github.com/coreos/etcd/clientv3"
// 	"time"
// )

// var (
// 	dialTimeout    = 2 * time.Second
// 	requestTimeout = 10 * time.Second
// )

// func Run() {
// 	ctx, _ := context.WithTimeout(context.Background(), requestTimeout)
// 	cli, _ := clientv3.New(clientv3.Config{
// 		DialTimeout: dialTimeout,
// 		Endpoints:   []string{"0.0.0.0:2379"},
// 	})
// 	defer cli.Close()
// 	kv := clientv3.NewKV(cli)

// 	//GetSingleValueDemo(ctx, kv)
// 	//PutValues(ctx, kv)
// 	// GetTopology(ctx, kv)
// 	//PutData(ctx, kv)

// 	GetTopologyData(ctx, kv)
// }

// func GetSingleValueDemo(ctx context.Context, kv clientv3.KV) {
// 	fmt.Println("*** GetSingleValueDemo()")
// 	// Delete all keys
// 	kv.Delete(ctx, "key", clientv3.WithPrefix())

// 	// Insert a key value
// 	pr, _ := kv.Put(ctx, "key", "444")
// 	rev := pr.Header.Revision
// 	fmt.Println("Revision:", rev)

// 	gr, _ := kv.Get(ctx, "key")
// 	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)

// 	// Modify the value of an existing key (create new revision)
// 	kv.Put(ctx, "key", "555")

// 	gr, _ = kv.Get(ctx, "key")
// 	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)

// 	// Get the value of the previous revision
// 	gr, _ = kv.Get(ctx, "key", clientv3.WithRev(rev))
// 	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)
// }

// func GetTopology(ctx context.Context, kv clientv3.KV) {
// 	gr, err := kv.Get(ctx, "/topology/nodes/congi", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))

// 	if err != nil {
// 		fmt.Println("ERROR!")
// 	}

// 	for _, item := range gr.Kvs {
// 		fmt.Println("K:%s, V:%s", string(item.Key), string(item.Value))
// 	}

// }

// func PutValues(ctx context.Context, kv clientv3.KV) {
// 	nodesKey := "/topology/nodes/congis"

// 	pr, _ := kv.Put(ctx, nodesKey+"node1", "444")
// 	rev := pr.Header.Revision
// 	fmt.Println("Revision:", rev)

// 	pr, _ = kv.Put(ctx, nodesKey+"node2", "555")
// 	rev = pr.Header.Revision
// 	fmt.Println("Revision:", rev)

// }

// func CreateDummy() (string, string) {
// 	l1 := model.KVS{
// 		Kvs: map[string]string{"l1": "v1", "l2": "v2"},
// 	}

// 	l2 := model.KVS{
// 		Kvs: map[string]string{"l1": "v1", "l3": "v4"},
// 	}

// 	c1 := model.KVS{
// 		Kvs: map[string]string{"c1": "v1"},
// 	}

// 	s1 := model.KVS{
// 		Kvs: map[string]string{"s1": "v1s"},
// 	}

// 	node1 := model.Node{
// 		Labels:  l1,
// 		Configs: c1,
// 		Secrets: s1,
// 		Jobs:    []model.Job{},
// 	}

// 	node2 := model.Node{
// 		Labels:  l2,
// 		Configs: c1,
// 		Secrets: s1,
// 		Jobs:    []model.Job{},
// 	}

// 	n1, _ := helper.NodeMarshall(node1)
// 	n2, _ := helper.NodeMarshall(node2)

// 	return string(n1), string(n2)
// }

// func PutData(ctx context.Context, kv clientv3.KV) {
// 	nodesKey := "/topology/novisad/grbavica/"
// 	n1, n2 := CreateDummy()

// 	pr, _ := kv.Put(ctx, nodesKey+"node1", n1)
// 	rev := pr.Header.Revision
// 	fmt.Println("Revision:", rev)

// 	pr, _ = kv.Put(ctx, nodesKey+"node2", n2)
// 	rev = pr.Header.Revision
// 	fmt.Println("Revision:", rev)
// }

// func GetTopologyData(ctx context.Context, kv clientv3.KV) {
// 	gr, err := kv.Get(ctx, "/topology/novisad/grbavica/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))

// 	if err != nil {
// 		fmt.Println("ERROR!")
// 	}

// 	for _, item := range gr.Kvs {
// 		fmt.Println("K:%s, V:%s", string(item.Key), string(item.Value))
// 	}

// }
