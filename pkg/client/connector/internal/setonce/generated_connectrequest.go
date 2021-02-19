// Code generated by "go generate". DO NOT EDIT.

package setonce

import (
	"context"
	"sync"

	"github.com/datawire/telepresence2/rpc/v2/connector"
)

type ConnectRequest interface {
	Get(ctx context.Context) (val *connector.ConnectRequest, ok bool)
	Set(val *connector.ConnectRequest)
}

type _ConnectRequest struct {
	val   *connector.ConnectRequest
	panic bool
	once  sync.Once
	ch    chan struct{}
}

func NewConnectRequest(second Behavior) ConnectRequest {
	return &_ConnectRequest{
		ch:    make(chan struct{}),
		panic: second.isPanic(),
	}
}

func (mu *_ConnectRequest) Set(val *connector.ConnectRequest) {
	didSet := false
	mu.once.Do(func() {
		mu.val = val
		close(mu.ch)
		didSet = true
	})
	if mu.panic && !didSet {
		panic("setonce.ConnectRequest.Set called multiple times")
	}
}

func (mu *_ConnectRequest) Get(ctx context.Context) (val *connector.ConnectRequest, ok bool) {
	select {
	case <-mu.ch:
		val = mu.val
		ok = true
	case <-ctx.Done():
		ok = false
	}
	return
}