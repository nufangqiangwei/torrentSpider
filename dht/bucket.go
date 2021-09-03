package dht

import "testTorrent/dht/int160"

type bucket struct {
	nodes map[*Node]struct{}
}

func (b *bucket) Len() int {
	return len(b.nodes)
}

func (b *bucket) EachNode(f func(*Node) bool) bool {
	for n := range b.nodes {
		if !f(n) {
			return false
		}
	}
	return true
}

func (b *bucket) AddNode(n *Node, k int) {
	if _, ok := b.nodes[n]; ok {
		return
	}
	if b.nodes == nil {
		b.nodes = make(map[*Node]struct{}, k)
	}
	b.nodes[n] = struct{}{}
}

func (b *bucket) GetNode(addr Addr, id int160.T) *Node {
	for n := range b.nodes {
		if n.hasAddrAndID(addr, id) {
			return n
		}
	}
	return nil
}
