package internal

import (
	"sync"
)

// TODO: Replace with optimisstic lock decoupling
type nodeRWLocker struct {
	sync.RWMutex
}

func (nl *nodeRWLocker) UpgradeLock() {
	// TODO: Race condition may occur here, due to lack of atomic upgrade
	// As soon as we release RLock, another goroutine may acquire Lock
	// before we acquire Lock again
	// nl.RUnlock()
	// nl.Lock()
	panic("not implemented")
}

var _ INodeLocker = (*nodeRWLocker)(nil)

func NewNodeLocker() INodeLocker {
	return new(nodeRWLocker)
}
