package etcd

import (
	"context"
	"fmt"
	"github.com/c12s/celestial/helper"
	cPb "github.com/c12s/scheme/celestial"
	sg "github.com/c12s/stellar-go"
	"github.com/golang/protobuf/proto"
)

func logMutate(ctx context.Context, req *cPb.MutateReq, db *DB) (string, error) {
	span, _ := sg.FromGRPCContext(ctx, "logMutate")
	defer span.Finish()
	fmt.Println(span)

	logKey := helper.TasksKey()
	data, err := proto.Marshal(req)
	if err != nil {
		span.AddLog(&sg.KV{"marshaling error", err.Error()})
		return "", err
	}

	chspan := span.Child("etcd.put log")
	_, err = db.Kv.Put(ctx, logKey, string(data))
	if err != nil {
		chspan.AddLog(&sg.KV{"etcd.put log error", err.Error()})
		return "", err
	}

	return logKey, nil
}
