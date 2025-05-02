package go_context_aware_lock

import "context"

type ICtxLock interface {
	AcquireCtx(ctx context.Context) error
	ReleaseCtx(ctx context.Context) error
}
