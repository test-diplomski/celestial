package service

import (
	"fmt"
	"github.com/c12s/celestial/helper"
	"github.com/c12s/celestial/model/config"
	"github.com/c12s/celestial/storage"
	bPb "github.com/c12s/scheme/blackhole"
	cPb "github.com/c12s/scheme/celestial"
	sg "github.com/c12s/stellar-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

type Server struct {
	db         storage.DB
	instrument map[string]string
	apollo     string
	meridian   string
}

func (s *Server) List(ctx context.Context, req *cPb.ListReq) (*cPb.ListResp, error) {
	span, _ := sg.FromGRPCContext(ctx, "celestial.list")
	defer span.Finish()
	fmt.Println(span)

	token, err := helper.ExtractToken(ctx)
	if err != nil {
		span.AddLog(&sg.KV{"token error", err.Error()})
		return nil, err
	}

	err = s.auth(ctx, listOpt(req, token))
	if err != nil {
		span.AddLog(&sg.KV{"auth error", err.Error()})
		return nil, err
	}

	_, err = s.checkNS(ctx, req.Extras["user"], req.Extras["namespace"])
	if err != nil {
		span.AddLog(&sg.KV{"check ns error", err.Error()})
		return nil, err
	}

	switch req.Kind {
	case cPb.ReqKind_SECRETS:
		err, resp := s.db.Secrets().List(
			helper.AppendToken(
				sg.NewTracedGRPCContext(ctx, span),
				token,
			),
			req.Extras,
		)
		if err != nil {
			span.AddLog(&sg.KV{"secrets list error", err.Error()})
			return nil, err
		}
		return resp, nil
	case cPb.ReqKind_ACTIONS:
		err, resp := s.db.Actions().List(
			helper.AppendToken(
				sg.NewTracedGRPCContext(ctx, span),
				token,
			),
			req.Extras,
		)
		if err != nil {
			span.AddLog(&sg.KV{"action list error", err.Error()})
			return nil, err
		}
		return resp, nil
	case cPb.ReqKind_CONFIGS:
		err, resp := s.db.Configs().List(
			helper.AppendToken(
				sg.NewTracedGRPCContext(ctx, span),
				token,
			),
			req.Extras,
		)
		if err != nil {
			span.AddLog(&sg.KV{"configs list error", err.Error()})
			return nil, err
		}
		return resp, nil
	}
	return &cPb.ListResp{Error: "Not valid file type"}, nil
}

func (s *Server) Mutate(ctx context.Context, req *cPb.MutateReq) (*cPb.MutateResp, error) {
	span, _ := sg.FromGRPCContext(ctx, "celestial.mutate")
	defer span.Finish()
	fmt.Println(span)

	token, err := helper.ExtractToken(ctx)
	if err != nil {
		span.AddLog(&sg.KV{"token error", err.Error()})
		return nil, err
	}

	err = s.auth(ctx, mutateOpt(req, token))
	if err != nil {
		span.AddLog(&sg.KV{"auth error", err.Error()})
		return nil, err
	}

	_, err = s.checkNS(ctx, req.Mutate.UserId, req.Mutate.Namespace)
	if err != nil {
		span.AddLog(&sg.KV{"check ns error", err.Error()})
		return nil, err
	}

	switch req.Mutate.Kind {
	case bPb.TaskKind_SECRETS:
		err, resp := s.db.Secrets().Mutate(
			helper.AppendToken(
				sg.NewTracedGRPCContext(ctx, span),
				token,
			), req,
		)
		if err != nil {
			span.AddLog(&sg.KV{"secrets mutate error", err.Error()})
			return nil, err
		}
		return resp, nil
	case bPb.TaskKind_ACTIONS:
		err, resp := s.db.Actions().Mutate(
			helper.AppendToken(
				sg.NewTracedGRPCContext(ctx, span),
				token,
			), req,
		)
		if err != nil {
			span.AddLog(&sg.KV{"actions mutate error", err.Error()})
			return nil, err
		}
		return resp, nil
	case bPb.TaskKind_CONFIGS:
		err, resp := s.db.Configs().Mutate(
			helper.AppendToken(
				sg.NewTracedGRPCContext(ctx, span),
				token,
			), req,
		)
		if err != nil {
			span.AddLog(&sg.KV{"configs mutate error", err.Error()})
			return nil, err
		}
		return resp, nil
	}
	return &cPb.MutateResp{Error: "Not valid file type"}, nil
}

func Run(db storage.DB, conf *config.Config) {
	lis, err := net.Listen("tcp", conf.Address)
	if err != nil {
		log.Fatalf("failed to initializa TCP listen: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	celestialServer := &Server{
		db:         db,
		instrument: conf.InstrumentConf,
		apollo:     conf.Apollo,
		meridian:   conf.Meridian,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n, err := sg.NewCollector(celestialServer.instrument["address"], celestialServer.instrument["stopic"])
	if err != nil {
		fmt.Println(err)
		return
	}
	c, err := sg.InitCollector(celestialServer.instrument["location"], n)
	if err != nil {
		fmt.Println(err)
		return
	}
	go c.Start(ctx, 15*time.Second)
	db.Reconcile().Start(ctx, conf.Gravity)

	fmt.Println("Celestial RPC Started")
	cPb.RegisterCelestialServiceServer(server, celestialServer)
	server.Serve(lis)
}
