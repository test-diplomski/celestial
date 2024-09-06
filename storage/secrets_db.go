package storage

import (
	"context"
)

type SecretsDB interface {
	SSecrets() SSecrets
}

type SSecrets interface {
	List(ctx context.Context, path, user string) (error, map[string]string)
	Mutate(ctx context.Context, key, user string, req map[string]interface{}) (error, string)
}
