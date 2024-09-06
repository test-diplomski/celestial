package etcd

import (
	"context"
	"errors"
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
	"strconv"
	"strings"
)

type Actions struct {
	db *DB
}

func construct(key string, nsTask *rPb.KV) *cPb.Data {
	data := &cPb.Data{Data: map[string]string{}}

	keyParts := strings.Split(key, "/")
	data.Data["regionid"] = keyParts[3]
	data.Data["clusterid"] = keyParts[4]
	data.Data["nodeid"] = keyParts[5]

	actions := []string{}
	for k, v := range nsTask.Extras {
		kv := strings.Join([]string{k, v.Value}, ":")
		actions = append(actions, kv)
	}
	actions = append(actions, strings.Join(nsTask.Index, ","))

	timestamp := helper.TSToString(nsTask.Timestamp)
	tkey := strings.Join([]string{"timestamp", timestamp}, "_")
	data.Data[tkey] = strings.Join(actions, ",")
	data.Data["index"] = strings.Join(nsTask.Index, ",")

	return data
}

func (a *Actions) getHT(ctx context.Context, key string, head, tail int64) (error, *cPb.Data) {
	span, _ := sg.FromGRPCContext(ctx, "head/tail")
	defer span.Finish()
	fmt.Println(span)

	if head != 0 {
		chspan := span.Child("etcd.get head")
		gresp, err := a.db.Kv.Get(ctx, key, clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend), clientv3.WithLimit(head))
		if err != nil {
			chspan.AddLog(&sg.KV{"etcd get head error", err.Error()})
			return err, nil
		}
		for _, item := range gresp.Kvs {
			nsTask := &rPb.KV{}
			err = proto.Unmarshal(item.Value, nsTask)
			if err != nil {
				span.AddLog(&sg.KV{"unmarshall etcd get head error", err.Error()})
				return err, nil
			}
			return nil, construct(key, nsTask)
		}
		go chspan.Finish()

	} else if tail != 0 {
		chspan := span.Child("etcd.get tail")
		gresp, err := a.db.Kv.Get(ctx, key, clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(tail))
		if err != nil {
			chspan.AddLog(&sg.KV{"etcd get tail error", err.Error()})
			return err, nil
		}
		for _, item := range gresp.Kvs {
			nsTask := &rPb.KV{}
			err = proto.Unmarshal(item.Value, nsTask)
			if err != nil {
				span.AddLog(&sg.KV{"unmarshall etcd get tail error", err.Error()})
				return err, nil
			}
			return nil, construct(key, nsTask)
		}
		go chspan.Finish()
	}

	span.AddLog(&sg.KV{"request error", "Cant use hand and tail at the same time!"})
	return errors.New("Cant use hand and tail at the same time!"), nil
}

func (a *Actions) getFT(ctx context.Context, key string, from, to int64) (error, *cPb.Data) {
	span, _ := sg.FromGRPCContext(ctx, "filter")
	defer span.Finish()
	fmt.Println(span)

	chspan := span.Child("etcd.get")
	data := &cPb.Data{Data: map[string]string{}}
	gresp, err := a.db.Kv.Get(ctx, key,
		clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		chspan.AddLog(&sg.KV{"etcd get error", err.Error()})
		return err, nil
	}
	go chspan.Finish()

	for _, item := range gresp.Kvs {
		nsTask := &rPb.KV{}
		err = proto.Unmarshal(item.Value, nsTask)
		if err != nil {
			span.AddLog(&sg.KV{"unmarshall etcd get error", err.Error()})
			return err, nil
		}

		keyParts := strings.Split(key, "/")
		data.Data["regionid"] = keyParts[3]
		data.Data["clusterid"] = keyParts[4]
		data.Data["nodeid"] = keyParts[5]

		actions := []string{}
		for k, v := range nsTask.Extras {
			kv := strings.Join([]string{k, v.Value}, ":")
			if from != 0 && to != 0 {
				if nsTask.Timestamp >= from && nsTask.Timestamp <= to {
					actions = append(actions, kv)
				}
			} else if from != 0 && to == 0 {
				if nsTask.Timestamp >= from {
					actions = append(actions, kv)
				}
			} else if from == 0 && to != 0 {
				if nsTask.Timestamp <= to {
					actions = append(actions, kv)
				}
			} else {
				actions = append(actions, kv)
			}
		}
		timestamp := helper.TSToString(nsTask.Timestamp)
		tkey := strings.Join([]string{"timestamp", timestamp}, "_")
		data.Data[tkey] = strings.Join(actions, ",")
		data.Data["index"] = strings.Join(nsTask.Index, ",")
	}
	return nil, data
}

func getExtras(extras map[string]string) (int64, int64, int64, int64) {
	head := int64(0)
	tail := int64(0)
	from := int64(0)
	to := int64(0)

	if val, ok := extras["head"]; ok {
		head, _ = strconv.ParseInt(val, 10, 64)
	}

	if val, ok := extras["tail"]; ok {
		tail, _ = strconv.ParseInt(val, 10, 64)
	}

	if val, ok := extras["from"]; ok {
		from, _ = strconv.ParseInt(val, 10, 64)
	}

	if val, ok := extras["to"]; ok {
		to, _ = strconv.ParseInt(val, 10, 64)
	}

	return head, tail, from, to
}

func (a *Actions) get(ctx context.Context, key string, head, tail, from, to int64) (error, *cPb.Data) {
	span, _ := sg.FromGRPCContext(ctx, "get")
	defer span.Finish()
	fmt.Println(span)

	if head != 0 || tail != 0 {
		return a.getHT(sg.NewTracedGRPCContext(ctx, span), key, head, tail)
	}
	return a.getFT(sg.NewTracedGRPCContext(ctx, span), key, from, to)
}

func (a *Actions) List(ctx context.Context, extras map[string]string) (error, *cPb.ListResp) {
	span, _ := sg.FromGRPCContext(ctx, "list")
	defer span.Finish()
	fmt.Println(span)

	chspan := span.Child("etcd.get searchLabels")
	searchLabelsKey := helper.JoinParts("", "topology", "regions", "labels") // -> topology/regions/labels => search key
	gresp, err := a.db.Kv.Get(ctx, searchLabelsKey, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		chspan.AddLog(&sg.KV{"etcd.get error", err.Error()})
		return err, nil
	}
	go chspan.Finish()

	cmp := extras["compare"]
	els := helper.SplitLabels(extras["labels"])
	head, tail, from, to := getExtras(extras)

	datas := []*cPb.Data{}
	for _, item := range gresp.Kvs {
		artefact := helper.NewNSArtifact(extras["user"], extras["namespace"], "actions")
		newKey := helper.NewKey(string(item.Key), artefact)
		ls := helper.SplitLabels(string(item.Value))
		switch cmp {
		case "all":
			if len(ls) == len(els) && helper.Compare(ls, els, true) {
				gerr, data := a.get(sg.NewTracedGRPCContext(ctx, span), newKey, head, tail, from, to)
				if gerr != nil {
					continue
				}
				if len(data.Data) > 0 {
					datas = append(datas, data)
				}
			}
		case "any":
			if helper.Compare(ls, els, false) {
				gerr, data := a.get(sg.NewTracedGRPCContext(ctx, span), newKey, head, tail, from, to)
				if gerr != nil {
					continue
				}
				if len(data.Data) > 0 {
					datas = append(datas, data)
				}
			}
		}
	}
	return nil, &cPb.ListResp{Data: datas}
}

// key -> topology/regions/actions/regionid/clusterid/nodes/nodeid
func (a *Actions) mutate(ctx context.Context, key, userId string, payloads []*bPb.Payload) error {
	span, _ := sg.FromGRPCContext(ctx, "helper mutate")
	defer span.Finish()
	fmt.Println(span)

	// Get what is current state of the configs for the node
	actions := &rPb.KV{
		Extras:    map[string]*rPb.KVData{},
		Timestamp: helper.Timestamp(),
		UserId:    userId,
	}

	// WHEN WORKING WITH ACTIONS WE NEED TO PRESEVER ACTIONS ORDER!
	for _, payload := range payloads {
		for _, index := range payload.Index {
			actions.Extras[index] = &rPb.KVData{payload.Value[index], "Waiting"}
		}
		actions.Index = payload.Index
	}

	// Save node actions
	aData, aerr := proto.Marshal(actions)
	if aerr != nil {
		span.AddLog(&sg.KV{"marshaling error", aerr.Error()})
		return aerr
	}

	chspan := span.Child("etcd.put")
	_, aerr = a.db.Kv.Put(ctx, key, string(aData))
	if aerr != nil {
		chspan.AddLog(&sg.KV{"etcd.put error", aerr.Error()})
		return aerr
	}
	chspan.Finish()

	return nil
}

func (a *Actions) Mutate(ctx context.Context, req *cPb.MutateReq) (error, *cPb.MutateResp) {
	span, _ := sg.FromGRPCContext(ctx, "mutate")
	defer span.Finish()
	fmt.Println(span)

	// Log mutate request for resilience
	logTaskKey, lerr := logMutate(sg.NewTracedGRPCContext(ctx, span), req, a.db)
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
	gresp, err := a.db.Kv.Get(ctx, searchLabelsKey, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		chspan.AddLog(&sg.KV{"etcd.get error", err.Error()})
		return err, nil
	}
	go chspan.Finish()

	for _, item := range gresp.Kvs {
		artefact := helper.NewNSArtifact(task.UserId, task.Namespace, "actions")
		key := helper.NewKey(string(item.Key), artefact)
		newKey := helper.Join(key, helper.TSToString(task.Timestamp))
		ls := helper.SplitLabels(string(item.Value))
		els := helper.Labels(task.Task.Selector.Labels)

		switch task.Task.Selector.Kind {
		case bPb.CompareKind_ALL:
			if len(ls) == len(els) && helper.Compare(ls, els, true) {
				err = a.mutate(sg.NewTracedGRPCContext(ctx, span), newKey, task.UserId, task.Task.Payload)
				if err != nil {
					span.AddLog(&sg.KV{"mutate error", err.Error()})
					return err, nil
				}
				index = append(index, newKey)
			}
		case bPb.CompareKind_ANY:
			if helper.Compare(ls, els, false) {
				err = a.mutate(sg.NewTracedGRPCContext(ctx, span), newKey, task.UserId, task.Task.Payload)
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
	err = a.sendToGravity(sg.NewTracedGRPCContext(ctx, span), req, logTaskKey)
	if err != nil {
		span.AddLog(&sg.KV{"putTask error", err.Error()})
	}

	span.AddLog(&sg.KV{"actions addition", "Actions added."})
	return nil, &cPb.MutateResp{"Actions added."}
}

func (a *Actions) sendToGravity(ctx context.Context, req *cPb.MutateReq, taskKey string) error {
	span, _ := sg.FromGRPCContext(ctx, "sendToGravity")
	defer span.Finish()
	fmt.Println(span)

	token, err := helper.ExtractToken(ctx)
	if err != nil {
		span.AddLog(&sg.KV{"token error", err.Error()})
		return err
	}

	client := service.NewGravityClient(a.db.Gravity)
	for _, key := range req.Index {
		span.AddLog(
			&sg.KV{"update key", key},
			&sg.KV{"update status", "In progress"},
		)
		a.StatusUpdate(sg.NewTracedGRPCContext(ctx, span), key, "In progress")
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

func (a *Actions) StatusUpdate(ctx context.Context, key, newStatus string) error {
	span, _ := sg.FromGRPCContext(ctx, "statusUpdate")
	defer span.Finish()
	fmt.Println(span)

	child := span.Child("etcd.get")
	resp, err := a.db.Kv.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		child.AddLog(&sg.KV{"etcd.get error", err.Error()})
		return err
	}
	go child.Finish()

	for _, item := range resp.Kvs {
		actions := &rPb.KV{}
		err = proto.Unmarshal(item.Value, actions)
		if err != nil {
			span.AddLog(&sg.KV{"unmarshall error", err.Error()})
			return err
		}
		for k, _ := range actions.Extras {
			kvc := actions.Extras[k]
			actions.Extras[k] = &rPb.KVData{kvc.Value, newStatus}
		}

		// Save node configs
		cData, err := proto.Marshal(actions)
		if err != nil {
			span.AddLog(&sg.KV{"marshaling error", err.Error()})
			return err
		}

		child1 := span.Child("etcd.put")
		_, err = a.db.Kv.Put(ctx, key, string(cData))
		if err != nil {
			child1.AddLog(&sg.KV{"etcd.put error", err.Error()})
			return err
		}
		child1.Finish()
	}

	return nil
}
