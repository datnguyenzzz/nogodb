package go_context_aware_lock

import "github.com/datnguyenzzz/nogodb/lib/go-context-aware-lock/local_lock"

func NewLocalLock() ICtxLock {
	return local_lock.NewLock()
}
