package helper

import (
	"context"
	"errors"
	"google.golang.org/grpc/metadata"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	labels = "labels"
	status = "status"

	topology = "topology"
	regions  = "regions"
	nodes    = "nodes"
	actions  = "actions"
	configs  = "configs"
	secrets  = "secrets"
	undone   = "undone"

	tasks = "tasks"
)

func Compare(a, b []string, strict bool) bool {
	for _, akv := range a {
		for _, bkv := range b {
			if akv == bkv && !strict {
				return true
			}
		}
	}
	return true
}

func Labels(lbs map[string]string) []string {
	lbls := []string{}
	for k, v := range lbs {
		kv := strings.Join([]string{k, v}, ":")
		lbls = append(lbls, kv)
	}
	sort.Strings(lbls)
	return lbls
}

func SplitLabels(value string) []string {
	ls := strings.Split(value, ",")
	sort.Strings(ls)
	return ls
}

/*
topology/regions/labels/regionid/clusterid/nodeid -> [k:v, k:v]
topology/regions/regionid/clusterid/nodes/nodeid -> {stats}

topology/regions/regionid/clusterid/nodeid/userid:namespace:configs -> {config list with status}
topology/regions/regionid/clusterid/nodeid/userid:namespace:secrets -> {secrets list with status}
topology/regions/userid:namespace:actions/regionid/clusterid/nodeid/timestamp -> {actions list history with status}

topology/regions/tasks/timestamp -> {tasks submited to particular cluster} holds data until all changes are commited! Append log
*/

// topology/regions/regionid/clusterid/nodeid -> [k:v, k:v]
func ACSNodeKey(rid, cid, nid string) string {
	s := []string{topology, regions, rid, cid, nid}
	return strings.Join(s, "/")
}

// topology/regions/regionid/clusterid/nodes
func ACSNodesKey(rid, cid string) string {
	s := []string{topology, regions, rid, cid, nodes}
	return strings.Join(s, "/")
}

// topology/regions/labels/regionid/clusterid/nodeid
func ACSLabelsKey(rid, cid, nid string) string {
	s := []string{topology, regions, labels, rid, cid, nid}
	return strings.Join(s, "/")
}

// topology/regionid/clusterid/nodeid/{artifact} [configs | secrets | actions]
func Join(keyPart, artifact string) string {
	s := []string{keyPart, artifact}
	return strings.Join(s, "/")
}

// construct variable path
func JoinParts(artifact string, parts ...string) string {
	s := []string{}
	for _, part := range parts {
		s = append(s, part)
	}

	if artifact != "" {
		s = append(s, artifact)
	}
	return strings.Join(s, "/")
}

func JoinFull(parts ...string) string {
	s := []string{}
	for _, part := range parts {
		s = append(s, part)
	}
	return strings.Join(s, "/")
}

func Timestamp() int64 {
	return time.Now().Unix()
}

// from: topology/regions/labels/regionid/clusterid/nodeid -> topology/regions/{replacement}/regionid/clusterid/nodeid
// {configs | secrets | actions}
func Key(path, replacement string) string {
	return strings.Replace(path, labels, replacement, -1)
}

func SearchKey(regionid, clusterid string) (string, error) {
	if regionid == "*" && clusterid == "*" {
		return JoinParts("", topology, regions, labels), nil // topology/regions/labels/
	} else if regionid != "*" && clusterid == "*" {
		return JoinParts("", topology, regions, labels, regionid), nil // topology/regions/labels/regionid/
	} else if regionid != "*" && clusterid != "*" { //topology/regions/labels/regionid/clusterid/
		return JoinParts("", topology, regions, labels, regionid, clusterid), nil
	}
	return "", errors.New("Request not valid")
}

// topology/regions/regionid/clusterid/nodeid/userid:namespace:artifact {configs | secrets | adtions}
func NewNSArtifact(userid, namespace, artifact string) string {
	return strings.Join([]string{userid, namespace, artifact}, ":")
}

func NewKey(path, artifact string) string {
	keyPart := strings.Join(strings.Split(path, "/labels/"), "/")
	newKey := Join(keyPart, artifact)
	return newKey
}

func TSToString(value int64) string {
	return strconv.FormatInt(value, 10)
}

func TasksKey() string {
	ts := TSToString(Timestamp())
	return JoinFull(topology, regions, tasks, ts)
}

func NodeKey(key string) string {
	return strings.TrimSuffix(NewKey(key, ""), "/")
}

func ConstructKey(node, kind string) string {
	dotted := strings.Join([]string{node, kind}, ".")
	return strings.ReplaceAll(dotted, ".", "/")
}

func ToUpper(s string) string {
	return strings.ToUpper(s)
}

func AppendToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "c12stoken", token)
}

func ExtractToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("No token in the request")
	}

	if _, ok := md["c12stoken"]; !ok {
		return "", errors.New("No token in the request")
	}

	return md["c12stoken"][0], nil
}
