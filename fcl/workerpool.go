package fcl

import (
	"context"
	"sync"
)

type WorkerPoolTask func(ctx context.Context)

type WorkerPool struct {
	lock       *sync.Mutex
	numRunners int
	wg         *sync.WaitGroup
	isClosed   bool
	ctx        context.Context
	ctxCancel  context.CancelFunc
	tasks      chan func()
}

func NewWorkerPool(ctxParent context.Context, numRunners int) *WorkerPool {
	ctx, ctxCancel := context.WithCancel(ctxParent)
	ret := &WorkerPool{
		numRunners: numRunners,
		tasks:      make(chan func(), 16),
		wg:         &sync.WaitGroup{},
		lock:       &sync.Mutex{},
		ctx:        ctx,
		ctxCancel:  ctxCancel,
	}
	for range numRunners {
		ret.wg.Go(ret.runner)
	}
	return ret
}

func (wp *WorkerPool) SubmitBackground(task WorkerPoolTask) bool {
	wp.lock.Lock()
	defer wp.lock.Unlock()
	if wp.isClosed {
		return false
	}
	select {
	case wp.tasks <- func() {
		select {
		case <-wp.ctx.Done():
			return
		default:
		}
		task(wp.ctx)
	}:
		return true
	default:
		return false
	}
}

func (wp *WorkerPool) SubmitAndWait(task WorkerPoolTask) bool {
	wp.lock.Lock()
	defer wp.lock.Unlock()
	if wp.isClosed {
		return false
	}
	var done sync.WaitGroup
	done.Add(1)
	select {
	case wp.tasks <- func() {
		select {
		case <-wp.ctx.Done():
			done.Done()
			return
		default:
		}
		task(wp.ctx)
		done.Done()
	}:
		done.Wait()
		return true
	default:
		return false
	}
}

func (wp *WorkerPool) runner() {
	for fn := range wp.tasks {
		fn()
	}
}

func (wp *WorkerPool) Close() {
	wp.lock.Lock()
	defer wp.lock.Unlock()
	wp.isClosed = true
	close(wp.tasks)
	wp.ctxCancel()
}
