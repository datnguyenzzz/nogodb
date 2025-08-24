package common

type InternalKV struct {
	K InternalKey
	// TODO (high): Need to re-evaluate, should we have to find a way to lazy-load the V instead ?
	//  because the InternalKV used during the iteration, by which the V might not need to be read at some points
	//  Lazy-load value: The V can either stored in the memory and immediately accessible, or it may
	//  be stored out-of-band and need to be fetched when  required.
	V []byte
}

func (ikv *InternalKV) Value() []byte {
	return ikv.V
}
