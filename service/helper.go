package service

import (
	"errors"
	"fmt"
	"github.com/c12s/celestial/helper"
	aPb "github.com/c12s/scheme/apollo"
	bPb "github.com/c12s/scheme/blackhole"
	cPb "github.com/c12s/scheme/celestial"
	mPb "github.com/c12s/scheme/meridian"
	sg "github.com/c12s/stellar-go"
	"golang.org/x/net/context"
	"strings"
)

func (s *Server) auth(ctx context.Context, opt *aPb.AuthOpt) error {
	span, _ := sg.FromGRPCContext(ctx, "auth")
	defer span.Finish()
	fmt.Println(span)

	token, err := helper.ExtractToken(ctx)
	if err != nil {
		span.AddLog(&sg.KV{"token error", err.Error()})
		return err
	}

	client := NewApolloClient(s.apollo)
	resp, err := client.Auth(
		helper.AppendToken(
			sg.NewTracedGRPCContext(ctx, span),
			token,
		),
		opt,
	)
	if err != nil {
		span.AddLog(&sg.KV{"apollo resp error", err.Error()})
		return err
	}

	if !resp.Value {
		span.AddLog(&sg.KV{"apollo.auth value", resp.Data["message"]})
		return errors.New(resp.Data["message"])
	}
	return nil
}

func (s *Server) checkNS(ctx context.Context, userid, namespace string) (string, error) {
	span, _ := sg.FromGRPCContext(ctx, "ns check")
	defer span.Finish()
	fmt.Println(span)

	client := NewMeridianClient(s.meridian)
	mrsp, err := client.Exists(sg.NewTracedGRPCContext(ctx, span),
		&mPb.NSReq{
			Name:   namespace,
			Extras: map[string]string{"userid": userid},
		},
	)
	if err != nil {
		span.AddLog(&sg.KV{"meridian exists error", err.Error()})
		return "", err
	}

	if mrsp.Extras["exists"] == "" {
		fmt.Println("namespace do not exists")
		return "", errors.New(fmt.Sprintf("%s do not exists", namespace))
	}
	fmt.Println("namespace exists")
	return mrsp.Extras["exists"], nil
}

func listKind(kind cPb.ReqKind) string {
	switch kind {
	case cPb.ReqKind_SECRETS:
		return "secrets"
	case cPb.ReqKind_ACTIONS:
		return "actions"
	case cPb.ReqKind_CONFIGS:
		return "configs"
	}
	return ""
}

func mutateKind(kind bPb.TaskKind) string {
	switch kind {
	case bPb.TaskKind_SECRETS:
		return "secrets"
	case bPb.TaskKind_ACTIONS:
		return "actions"
	case bPb.TaskKind_CONFIGS:
		return "configs"
	}
	return ""
}

func compareKind(kind bPb.CompareKind) string {
	switch kind {
	case bPb.CompareKind_ALL:
		return "kind:all"
	case bPb.CompareKind_ANY:
		return "kind:any"
	}
	return ""
}

func join(data map[string]string) string {
	temp := []string{}
	for lk, lv := range data {
		temp = append(temp, strings.Join([]string{lk, lv}, ":"))
	}
	return strings.Join(temp, ",")
}

func listOpt(req *cPb.ListReq, token string) *aPb.AuthOpt {
	return &aPb.AuthOpt{
		Data: map[string]string{
			"intent": "auth",
			"action": "list",
			"kind":   listKind(req.Kind),
			"token":  token,
		},
		Extras: map[string]*aPb.OptExtras{
			"user":      &aPb.OptExtras{Data: []string{req.Extras["user"]}},
			"namespace": &aPb.OptExtras{Data: []string{req.Extras["namespace"]}},
			"cmp":       &aPb.OptExtras{Data: []string{req.Extras["compare"]}},
			"labels":    &aPb.OptExtras{Data: []string{req.Extras["labels"]}},
		},
	}
}

func mutateOpt(req *cPb.MutateReq, token string) *aPb.AuthOpt {
	return &aPb.AuthOpt{
		Data: map[string]string{
			"intent":    "auth",
			"action":    "mutate",
			"kind":      mutateKind(req.Mutate.Kind),
			"user":      req.Mutate.UserId,
			"token":     token,
			"namespace": req.Mutate.Namespace,
		},
		Extras: map[string]*aPb.OptExtras{
			req.Mutate.Task.RegionId: &aPb.OptExtras{Data: []string{
				req.Mutate.Task.ClusterId,
				join(req.Mutate.Task.Selector.Labels),
				compareKind(req.Mutate.Task.Selector.Kind),
			},
			},
		},
	}
}
