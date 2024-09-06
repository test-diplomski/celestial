package etcd

import (
	"context"
	"fmt"
	"github.com/c12s/celestial/helper"
	"github.com/c12s/celestial/service"
	bPb "github.com/c12s/scheme/blackhole"
	cPb "github.com/c12s/scheme/celestial"
	rPb "github.com/c12s/scheme/core"
	gPb "github.com/c12s/scheme/gravity"
	sg "github.com/c12s/stellar-go"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"strings"
)

type Configs struct {
	db *DB
}

func (n *Configs) get(ctx context.Context, key string) (error, *cPb.Data) {
	span, _ := sg.FromGRPCContext(ctx, "get")
	defer span.Finish()
	fmt.Println(span)

	chspan := span.Child("etcd.get")
	gresp, err := n.db.Kv.Get(ctx, key)
	if err != nil {
		chspan.AddLog(&sg.KV{"etcd get error", err.Error()})
		return err, nil
	}
	go chspan.Finish()

	data := &cPb.Data{Data: map[string]string{}}
	for _, item := range gresp.Kvs {
		nsTask := &rPb.KV{}
		err = proto.Unmarshal(item.Value, nsTask)
		if err != nil {
			span.AddLog(&sg.KV{"unmarshall etcd get error", err.Error()})
			return err, nil
		}

		keyParts := strings.Split(key, "/")
		data.Data["regionid"] = keyParts[2]
		data.Data["clusterid"] = keyParts[3]
		data.Data["nodeid"] = keyParts[4]

		configs := []string{}
		for k, v := range nsTask.Extras {
			kv := strings.Join([]string{k, v.Value}, ":")
			configs = append(configs, kv)
		}
		data.Data["configs"] = strings.Join(configs, ",")
	}
	return nil, data
}

func (c *Configs) List(ctx context.Context, extras map[string]string) (error, *cPb.ListResp) {
	span, _ := sg.FromGRPCContext(ctx, "list")
	defer span.Finish()
	fmt.Println(span)

	chspan := span.Child("etcd.get searchLabels")
	searchLabelsKey := helper.JoinParts("", "topology", "regions", "labels") // -> topology/regions/labels => search key
	gresp, err := c.db.Kv.Get(ctx, searchLabelsKey, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		chspan.AddLog(&sg.KV{"etcd.get error", err.Error()})
		return err, nil
	}
	go chspan.Finish()

	cmp := extras["compare"]
	els := helper.SplitLabels(extras["labels"])

	datas := []*cPb.Data{}
	for _, item := range gresp.Kvs {
		artefact := helper.NewNSArtifact(extras["user"], extras["namespace"], "configs")
		newKey := helper.NewKey(string(item.Key), artefact)
		ls := helper.SplitLabels(string(item.Value))
		switch cmp {
		case "all":
			if len(ls) == len(els) && helper.Compare(ls, els, true) {
				gerr, data := c.get(sg.NewTracedGRPCContext(ctx, span), newKey)
				if gerr != nil {
					continue
				}
				datas = append(datas, data)
			}
		case "any":
			if helper.Compare(ls, els, false) {
				gerr, data := c.get(sg.NewTracedGRPCContext(ctx, span), newKey)
				if gerr != nil {
					continue
				}
				datas = append(datas, data)
			}
		}
	}
	return nil, &cPb.ListResp{Data: datas}
}

// key -> topology/regions/configs/regionid/clusterid/nodes/nodeid
func (c *Configs) mutate(ctx context.Context, key, userId string, payloads []*bPb.Payload) error {
	span, _ := sg.FromGRPCContext(ctx, "helper mutate")
	defer span.Finish()
	fmt.Println(span)

	configs := &rPb.KV{
		Extras:    map[string]*rPb.KVData{},
		Timestamp: helper.Timestamp(),
		UserId:    userId,
	}

	// Clear previous values, so that new values can take an effect
	for _, payload := range payloads {
		for pk, pv := range payload.Value {
			configs.Extras[pk] = &rPb.KVData{pv, "Waiting"}
		}
	}

	// Save node configs
	cData, err := proto.Marshal(configs)
	if err != nil {
		span.AddLog(&sg.KV{"marshaling error", err.Error()})
		return err
	}

	chspan := span.Child("etcd.put")
	_, err = c.db.Kv.Put(ctx, key, string(cData))
	if err != nil {
		chspan.AddLog(&sg.KV{"etcd.put error", err.Error()})
		return err
	}
	chspan.Finish()

	return nil
}

func (c *Configs) Mutate(ctx context.Context, req *cPb.MutateReq) (error, *cPb.MutateResp) {
	span, _ := sg.FromGRPCContext(ctx, "mutate")
	defer span.Finish()
	fmt.Println(span)

	// Log mutate request for resilience
	logTaskKey, lerr := logMutate(sg.NewTracedGRPCContext(ctx, span), req, c.db)
	if lerr != nil {
		span.AddLog(&sg.KV{"resilience log error", lerr.Error()})
		return lerr, nil
	}

	task := req.Mutate
	searchLabelsKey, kerr := helper.SearchKey(task.Task.RegionId, task.Task.ClusterId)
	if kerr != nil {
		span.AddLog(&sg.KV{"search key error", kerr.Error()})
		return kerr, nil
	}
	index := []string{}

	chspan := span.Child("etcd.get searchLabels")
	gresp, err := c.db.Kv.Get(ctx, searchLabelsKey, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		chspan.AddLog(&sg.KV{"etcd.get error", err.Error()})
		return err, nil
	}
	go chspan.Finish()

	for _, item := range gresp.Kvs {
		artefact := helper.NewNSArtifact(task.UserId, task.Namespace, "configs")
		newKey := helper.NewKey(string(item.Key), artefact)
		ls := helper.SplitLabels(string(item.Value))
		els := helper.Labels(task.Task.Selector.Labels)

		switch task.Task.Selector.Kind {
		case bPb.CompareKind_ALL:
			if len(ls) == len(els) && helper.Compare(ls, els, true) {
				err = c.mutate(sg.NewTracedGRPCContext(ctx, span), newKey, task.UserId, task.Task.Payload)
				if err != nil {
					span.AddLog(&sg.KV{"mutate error", err.Error()})
					return err, nil
				}
				index = append(index, newKey)
			}
		case bPb.CompareKind_ANY:
			if helper.Compare(ls, els, false) {
				err = c.mutate(sg.NewTracedGRPCContext(ctx, span), newKey, task.UserId, task.Task.Payload)
				if err != nil {
					span.AddLog(&sg.KV{"mutate error", err.Error()})
					return err, nil
				}
				index = append(index, newKey)
			}
		}
	}

	//Save index for gravity
	req.Index = index
	err = c.sendToGravity(sg.NewTracedGRPCContext(ctx, span), req, logTaskKey)
	if err != nil {
		span.AddLog(&sg.KV{"putTask error", err.Error()})
	}

	span.AddLog(&sg.KV{"config addition", "Config added."})
	return nil, &cPb.MutateResp{"Config added."}
}

func (c *Configs) sendToGravity(ctx context.Context, req *cPb.MutateReq, taskKey string) error {
	span, _ := sg.FromGRPCContext(ctx, "sendToGravity")
	defer span.Finish()
	fmt.Println(span)
	fmt.Println("SERIALIZE ", span.Serialize())

	token, err := helper.ExtractToken(ctx)
	if err != nil {
		span.AddLog(&sg.KV{"token error", err.Error()})
		return err
	}

	client := service.NewGravityClient(c.db.Gravity)
	for _, key := range req.Index {
		span.AddLog(
			&sg.KV{"update key", key},
			&sg.KV{"update status", "In progress"},
		)
		c.StatusUpdate(sg.NewTracedGRPCContext(ctx, span), key, "In progress")
	}

	gReq := &gPb.PutReq{
		Key:     taskKey, //key to be deleted after push is done
		Task:    req,
		TaskKey: taskKey,
	}

	_, err = client.PutTask(
		helper.AppendToken(
			sg.NewTracedGRPCContext(ctx, span),
			token,
		),
		gReq,
	)
	if err != nil {
		span.AddLog(&sg.KV{"putTask error", err.Error()})
		return err
	}

	return nil
}

func (c *Configs) StatusUpdate(ctx context.Context, key, newStatus string) error {
	var span sg.Spanner
	span, _ = sg.FromGRPCContext(ctx, "statusUpdate")
	if span == nil {
		span, _ = sg.FromContext(ctx, "statusUpdate")
	}
	defer span.Finish()
	fmt.Println(span)

	child := span.Child("etcd.get")
	resp, err := c.db.Kv.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		child.AddLog(&sg.KV{"etcd.get error", err.Error()})
		return err
	}
	go child.Finish()

	for _, item := range resp.Kvs {
		configs := &rPb.KV{}
		err = proto.Unmarshal(item.Value, configs)
		if err != nil {
			span.AddLog(&sg.KV{"unmarshall error", err.Error()})
			return err
		}
		for k, _ := range configs.Extras {
			kvc := configs.Extras[k]
			configs.Extras[k] = &rPb.KVData{kvc.Value, newStatus}
		}

		// Save node configs
		cData, err := proto.Marshal(configs)
		if err != nil {
			span.AddLog(&sg.KV{"marshaling error", err.Error()})
			return err
		}

		child1 := span.Child("etcd.put")
		_, err = c.db.Kv.Put(ctx, key, string(cData))
		if err != nil {
			child1.AddLog(&sg.KV{"etcd.put error", err.Error()})
			return err
		}
		child1.Finish()
	}

	return nil
}
