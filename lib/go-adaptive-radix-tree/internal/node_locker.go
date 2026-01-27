package internal

import "sync"

type nodeLocker struct {
	sync.RWMutex // TODO: Replace with optimisstic lock decoupling
}
