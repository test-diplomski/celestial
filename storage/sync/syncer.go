package syncer

import (
	fPb "github.com/c12s/scheme/flusher"
)

type Syncer interface {
	Sub(f func(msg *fPb.Update))
}
