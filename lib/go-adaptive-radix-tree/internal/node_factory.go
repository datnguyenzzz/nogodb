package internal

import "fmt"

func NewNode[V any](k Kind) INode[V] {
	switch k {
	case KindNode4:
		return new(Node4[V])
	case KindNode16:
		return new(Node16[V])
	case KindNode48:
		return new(Node48[V])
	case KindNode256:
		return new(Node256[V])
	case KindNodeLeaf:
		return new(NodeLeaf[V])
	default:
		panic(fmt.Sprintf("%v node is unsupported", k))
	}
}
