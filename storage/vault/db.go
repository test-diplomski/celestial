package vault

import (
	"github.com/c12s/celestial/storage"
	"github.com/hashicorp/vault/api"
	"time"
)

type DB struct {
	client        *api.Client
	apolloAddress string
}

func New(endpoints []string, timeout time.Duration, apolloService string) (*DB, error) {
	cli, err := api.NewClient(&api.Config{
		Address: endpoints[0],
	})
	if err != nil {
		return nil, err
	}

	return &DB{
		client:        cli,
		apolloAddress: apolloService,
	}, nil
}

func (db *DB) init(token string) {
	db.client.SetToken(token)
}

func (db *DB) revert() {
	db.client.ClearToken()
}

func (db *DB) Close() {}

func (db *DB) SSecrets() storage.SSecrets { return &SSecrets{db} }
