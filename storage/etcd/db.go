package etcd

import (
	"context"
	"fmt"
	"github.com/c12s/celestial/model/config"
	"github.com/c12s/celestial/storage"
	sync "github.com/c12s/celestial/storage/sync"
	"github.com/c12s/celestial/storage/sync/nats"
	"github.com/c12s/celestial/storage/vault"
	"github.com/coreos/etcd/clientv3"
	"time"
)

var (
	dialTimeout    = 2 * time.Second
	requestTimeout = 10 * time.Second
)

const (
	//topology/labels/regionid/clusterid/nodeid
	labels = "topology/regions/labels/%s/%s/%s"

	//topology/regionid/clusterid/nodes/nodeid
	nodeid = "topology/regions/%s/%s/nodes/%s"

	//topology/regionid/clusterid/nodeid/undone
	undone = "topology/regions/%s/%s/%s/undone"

	//topology/regionid/clusterid/nodeid/configs
	configs = "topology/regions/%s/%s/%s/configs"

	l1 = "l1:v1,l2:v2"
	l2 = "l1:v1,l2:v2,l3:v3"
	l3 = "l1:v1,l2:v2,l3:v3,l4:v4"
)

type DB struct {
	Kv      clientv3.KV
	Client  *clientv3.Client
	sdb     storage.SecretsDB
	s       sync.Syncer
	Gravity string
	Apollo  string
}

func key(rid, cid, nid, template string) string {
	return fmt.Sprintf(template, rid, cid, nid)
}

func (db *DB) Init() {
	ctx, _ := context.WithTimeout(context.Background(), requestTimeout)
	// Setup regions clusters and nodes
	db.Kv.Put(ctx, key("novisad", "grbavica", "node1", nodeid), "node1")
	db.Kv.Put(ctx, key("novisad", "grbavica", "node2", nodeid), "node2")
	db.Kv.Put(ctx, key("novisad", "grbavica", "node3", nodeid), "node3")

	db.Kv.Put(ctx, key("novisad", "liman3", "node1", nodeid), "node1")
	db.Kv.Put(ctx, key("novisad", "liman3", "node2", nodeid), "node2")
	db.Kv.Put(ctx, key("novisad", "liman3", "node3", nodeid), "node3")

	// Setup labels for nodes
	db.Kv.Put(ctx, key("novisad", "grbavica", "node1", labels), l1)
	db.Kv.Put(ctx, key("novisad", "grbavica", "node2", labels), l1)
	db.Kv.Put(ctx, key("novisad", "grbavica", "node3", labels), l2)

	db.Kv.Put(ctx, key("novisad", "liman3", "node1", labels), l1)
	db.Kv.Put(ctx, key("novisad", "liman3", "node2", labels), l2)
	db.Kv.Put(ctx, key("novisad", "liman3", "node3", labels), l3)
}

func New(conf *config.Config, timeout time.Duration) (*DB, error) {
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: timeout,
		Endpoints:   conf.Endpoints,
	})

	if err != nil {
		return nil, err
	}

	//Load secrets database
	sdb, err := vault.New(conf.SEndpoints, timeout, conf.Apollo)
	if err != nil {
		return nil, err
	}

	ns, err := nats.NewNatsSync(conf.Syncer, conf.STopic)
	if err != nil {
		return nil, err
	}

	return &DB{
		Kv:      clientv3.NewKV(cli),
		Client:  cli,
		sdb:     sdb,
		s:       ns,
		Gravity: conf.Gravity,
		Apollo:  conf.Apollo,
	}, nil
}

func (db *DB) Close() { db.Client.Close() }

func (db *DB) Secrets() storage.Secrets { return &Secrets{db} }

func (db *DB) Configs() storage.Configs { return &Configs{db} }

func (db *DB) Actions() storage.Actions { return &Actions{db} }

func (db *DB) Reconcile() storage.Reconcile { return &Reconcile{db} }
