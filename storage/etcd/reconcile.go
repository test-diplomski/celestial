package etcd

import (
	"context"
	"fmt"
	"github.com/c12s/celestial/helper"
	bPb "github.com/c12s/scheme/blackhole"
	fPb "github.com/c12s/scheme/flusher"
	sg "github.com/c12s/stellar-go"
)

type Reconcile struct {
	db *DB
}

func (r *Reconcile) update(ctx context.Context, key, status string, kind bPb.TaskKind) error {
	span, _ := sg.FromContext(ctx, "status update")
	defer span.Finish()
	fmt.Println(span)

	switch kind {
	case bPb.TaskKind_SECRETS:
		err := r.db.Secrets().StatusUpdate(sg.NewTracedContext(ctx, span), key, status)
		if err != nil {
			span.AddLog(&sg.KV{"state update error", err.Error()})
			return err
		}
	case bPb.TaskKind_ACTIONS:
		err := r.db.Actions().StatusUpdate(sg.NewTracedContext(ctx, span), key, status)
		if err != nil {
			span.AddLog(&sg.KV{"state update error", err.Error()})
			return err
		}
	case bPb.TaskKind_CONFIGS:
		err := r.db.Configs().StatusUpdate(sg.NewTracedContext(ctx, span), key, status)
		if err != nil {
			span.AddLog(&sg.KV{"state update error", err.Error()})
			return err
		}
	}
	return nil
}

func (r *Reconcile) Start(ctx context.Context, address string) {
	r.db.s.Sub(func(msg *fPb.Update) {
		go func(c context.Context, data *fPb.Update) {
			span, _ := sg.FromCustomSource(
				msg.SpanContext,
				msg.SpanContext.Baggage,
				"reconcile.update",
			)
			fmt.Println(span)
			defer span.Finish()

			//TODO: remove task from gravity or update
			//TODO: update node job status

			key := helper.ConstructKey(data.Node, data.Kind)
			kind := helper.ToUpper(data.Kind)
			value, ok := bPb.TaskKind_value[kind]
			if !ok {
				return
			}
			err := r.update(sg.NewTracedContext(ctx, span), key, "Done", bPb.TaskKind(value))
			if err != nil {
				return
			}

			fmt.Println()
			fmt.Print("GET celestial: ")
			fmt.Println(msg)
			fmt.Println()
		}(ctx, msg)
	})
	fmt.Println("Started")
}
