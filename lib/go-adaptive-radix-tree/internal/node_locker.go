package internal

import (
	"sync"
)

type LockerType byte

const (
	RWMutexLocker LockerType = iota
	OptimissticRWMutexLocker
)

// TODO: Replace with optimisstic lock decoupling
type nodeRWLocker struct {
	sync.RWMutex
}

// // For debugging
// func (nl *nodeRWLocker) Lock() {
// 	// fmt.Printf("[%v] Lock-ing - %p\n", Goid(), nl)
// 	nl.RWMutex.Lock()
// 	fmt.Printf("[%v] 	Lock-ed - %p\n", Goid(), nl)
// }
// func (nl *nodeRWLocker) Unlock() {
// 	// fmt.Printf("[%v] Unlock-ing - %p\n", Goid(), nl)
// 	nl.RWMutex.Unlock()
// 	fmt.Printf("[%v] 	Unlock-ed - %p\n", Goid(), nl)
// }
// func (nl *nodeRWLocker) RLock() {
// 	// fmt.Printf("[%v] RLock-ing - %p\n", Goid(), nl)
// 	nl.RWMutex.RLock()
// 	fmt.Printf("[%v] 	RLock-ed - %p\n", Goid(), nl)
// }
// func (nl *nodeRWLocker) RUnlock() {
// 	// fmt.Printf("[%v] RUnlock-ing - %p\n", Goid(), nl)
// 	nl.RWMutex.RUnlock()
// 	fmt.Printf("[%v] 	RUnlock-ed - %p\n", Goid(), nl)
// }

func (nl *nodeRWLocker) UpgradeLock() {
	// TODO: Race condition may occur here, due to lack of atomic upgrade
	// As soon as we release RLock, another goroutine may acquire Lock
	// before we acquire Lock again
	// nl.RUnlock()
	// nl.Lock()
	panic("not implemented")
}

var _ INodeLocker = (*nodeRWLocker)(nil)

func NewNodeLocker(lockerType LockerType) INodeLocker {
	switch lockerType {
	case RWMutexLocker:
		return new(nodeRWLocker)
	default:
		panic("unsupported locker type")
	}
}
