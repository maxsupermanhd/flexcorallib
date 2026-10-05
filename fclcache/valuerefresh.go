package fclcache

import (
	"context"
	"sync"
	"time"

	"github.com/maxsupermanhd/flexcorallib/fcl"
)

type ValueRefresh[T any] struct {
	lock            sync.Mutex
	lastRefresh     time.Time
	refreshInterval time.Duration
	value           T
	valueErr        error
	getValueFn      func(context.Context) (T, error)
	wp              *fcl.WorkerPool
	refreshing      bool
	refreshDone     chan struct{}
}

func NewValueRefresh[T any](wp *fcl.WorkerPool, interval time.Duration, getValueFn func(context.Context) (T, error)) *ValueRefresh[T] {
	return &ValueRefresh[T]{
		wp:              wp,
		refreshInterval: interval,
		getValueFn:      getValueFn,
	}
}

// blocks until gets refreshed value
func (r *ValueRefresh[T]) Get(ctx context.Context) (T, error) {
	r.lock.Lock()
	for r.refreshing {
		done := r.refreshDone
		r.lock.Unlock()
		<-done
		r.lock.Lock()
	}
	if time.Since(r.lastRefresh) < r.refreshInterval {
		retval, reterr := r.value, r.valueErr
		r.lock.Unlock()
		return retval, reterr
	}

	r.acquireValue(ctx)

	retval, reterr := r.value, r.valueErr
	r.lock.Unlock()
	return retval, reterr
}

// immediately gets whatever is stored
func (r *ValueRefresh[T]) GetWhatever() (T, error) {
	r.lock.Lock()
	retval, reterr := r.value, r.valueErr
	r.lock.Unlock()
	return retval, reterr
}

type ValueCacheCommon interface {
	Ready() bool
	Refresh(ctx context.Context)
	LastRefresh() time.Time
}

func (r *ValueRefresh[T]) LastRefresh() time.Time {
	r.lock.Lock()
	ret := r.lastRefresh
	r.lock.Unlock()
	return ret
}

func (r *ValueRefresh[T]) Ready() bool {
	r.lock.Lock()
	ret := !r.lastRefresh.IsZero() && time.Since(r.lastRefresh)+1*time.Second < r.refreshInterval
	r.lock.Unlock()
	return ret
}

// assumes lock is initially locked, will unlock while refreshing and keep locked once done
func (r *ValueRefresh[T]) acquireValue(ctx context.Context) {
	r.refreshing = true
	r.refreshDone = make(chan struct{})
	r.lock.Unlock()

	v, verr := r.getValueFn(ctx)

	r.lock.Lock()
	r.value = v
	r.valueErr = verr
	r.lastRefresh = time.Now()
	r.refreshing = false
	if r.refreshDone != nil {
		close(r.refreshDone)
	}
}

// blocks only if worker pool is disfunctional
func (r *ValueRefresh[T]) Refresh(ctx context.Context) {
	r.lock.Lock()
	if time.Since(r.lastRefresh) < r.refreshInterval || r.refreshing {
		r.lock.Unlock()
		return
	}
	if !r.wp.SubmitBackground(func(ctxWorker context.Context) {
		r.acquireValue(ctxWorker)
		r.lock.Unlock()
	}) {
		r.acquireValue(ctx)
		r.lock.Unlock()
	}
}
