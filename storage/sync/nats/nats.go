package nats

import (
	fPb "github.com/c12s/scheme/flusher"
	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/nats.go"
)

type NatsSync struct {
	nc    *nats.Conn
	topic string
}

func NewNatsSync(address, topic string) (*NatsSync, error) {
	nc, err := nats.Connect(address)
	if err != nil {
		return nil, err
	}

	return &NatsSync{
		nc:    nc,
		topic: topic,
	}, nil
}

func (ns *NatsSync) Sub(f func(u *fPb.Update)) {
	ns.nc.Subscribe(ns.topic, func(msg *nats.Msg) {
		data := &fPb.Update{}
		err := proto.Unmarshal(msg.Data, data)
		if err != nil {
			f(nil)
		}
		f(data)
	})
	ns.nc.Flush()
}
