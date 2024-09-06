package vault

import (
	"context"
	"fmt"
	"github.com/c12s/celestial/service"
	aPb "github.com/c12s/scheme/apollo"
	sg "github.com/c12s/stellar-go"
	"strings"
)

type SSecrets struct {
	db *DB
}

const (
	keyPrefix = "secret"
)

//ckv/data/topology/regionid/clusterid/nodes/nodeid/secrets
func formatKey(path string) string {
	s := []string{keyPrefix, path}
	return strings.Join(s, "/")
}

func (s *SSecrets) getToken(ctx context.Context, user string) (error, string) {
	span, _ := sg.FromGRPCContext(ctx, "getToken")
	defer span.Finish()
	fmt.Println(span)

	client := service.NewApolloClient(s.db.apolloAddress)
	resp, err := client.GetToken(sg.NewTracedGRPCContext(ctx, span), &aPb.GetReq{user})
	if err != nil {
		span.AddLog(&sg.KV{"get token error", err.Error()})
		return err, ""
	}
	return nil, resp.Token
}

func (s *SSecrets) List(ctx context.Context, key, user string) (error, map[string]string) {
	span, _ := sg.FromGRPCContext(ctx, "list")
	defer span.Finish()
	fmt.Println(span)

	uerr, userId := s.getToken(sg.NewTracedGRPCContext(ctx, span), user)
	if uerr != nil {
		span.AddLog(&sg.KV{"get token error", uerr.Error()})
		return uerr, nil
	}

	s.db.init(userId)
	defer s.db.revert()

	chspan := span.Child("vault.read")
	retVal := map[string]string{}
	path := formatKey(key)
	secretValues, err := s.db.client.Logical().Read(path)
	if err != nil {
		chspan.AddLog(&sg.KV{"vault.read error", err.Error()})
		return err, nil
	}
	go chspan.Finish()

	if secretValues != nil {
		for propName, propValue := range secretValues.Data {
			retVal[propName] = propValue.(string)
		}
	}
	return nil, retVal
}

func (s *SSecrets) Mutate(ctx context.Context, key, user string, req map[string]interface{}) (error, string) {
	span, _ := sg.FromGRPCContext(ctx, "mutate")
	defer span.Finish()
	fmt.Println(span)

	uerr, userId := s.getToken(sg.NewTracedGRPCContext(ctx, span), user)
	if uerr != nil {
		return uerr, ""
	}

	chspan1 := span.Child("vault.init")
	s.db.init(userId)
	defer s.db.revert()
	chspan1.Finish()

	chspan2 := span.Child("vault.write")
	path := formatKey(key)
	_, err := s.db.client.Logical().Write(path, req)
	if err != nil {
		chspan2.AddLog(&sg.KV{"vault.write error", err.Error()})
		return err, ""
	}
	chspan2.Finish()

	return nil, path
}
