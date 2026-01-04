package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type registry struct {
	chans  sync.Map
	ctx    context.Context
	cancel context.CancelFunc
}

type busCore struct {
	mu    sync.Mutex
	name  string
	input <-chan any
	subs  atomic.Value //[]chan<- any
	r     *registry
}

func initRegistry() *registry {
	r := registry{}
	r.chans = sync.Map{}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	return &r
}

func (r *registry) publish(name string, input <-chan any) error {
	newbus := &busCore{name: name, input: input, subs: atomic.Value{}, r: r}
	newbus.subs.Store(make([]chan<- any, 0))
	if _, ok := r.chans.LoadOrStore(name, newbus); ok {
		return fmt.Errorf("already have a same name putsub")
	}
	go newbus.run()
	return nil
}

func (r *registry) subscribe(name string) (<-chan any, error) {
	value, ok := r.chans.Load(name)
	if !ok {
		return nil, fmt.Errorf("failed to sub %s", name)
	}
	bc := value.(*busCore)
	return bc.sub(), nil
}

func (r *registry) close() {
	r.cancel()
}

func (bc *busCore) run() {
	defer func() {
		bc.r.chans.Delete(bc.name)
		subs := bc.subs.Load().([]chan<- any)
		for i := range subs {
			close(subs[i])
		}
	}()
	for {
		select {
		case m, ok := <-bc.input:
			if !ok {
				return
			}
			subs := bc.subs.Load().([]chan<- any)
			for i := range subs {
				select {
				case subs[i] <- m:
				default:
				}
			}
		case <-bc.r.ctx.Done():
			return
		}
	}
}

func (bc *busCore) sub() <-chan any {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	newCh := make(chan any, 10)
	subs := bc.subs.Load().([]chan<- any)
	newSubs := make([]chan<- any, len(subs), len(subs)+1)
	copy(newSubs, subs)
	newSubs = append(newSubs, newCh)
	bc.subs.Store(newSubs)
	return newCh
}
