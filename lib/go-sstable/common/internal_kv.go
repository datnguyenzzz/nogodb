package common

type InternalKV struct {
	K InternalKey
	// TODO (high): Need to re-evaluate, should we have to find a way to lazy-load the V instead ?
	//  because the InternalKV used during the iteration, by which the V might not need to be read at some points
	V []byte
}
