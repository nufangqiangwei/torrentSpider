package dht

import (
	"time"

	"testTorrent/dht/int160"
	"testTorrent/dht/krpc"
)

type nodeKey struct {
	Addr Addr
	Id   int160.T
}

type Node struct {
	nodeKey
	announceToken *string
	readOnly      bool

	lastGotQuery    time.Time // From the remote Node
	lastGotResponse time.Time // From the remote Node

	numReceivesFrom     int
	consecutiveFailures int
}

func (s *Server) IsQuestionable(n *Node) bool {
	return !s.IsGood(n) && !s.nodeIsBad(n)
}

func (n *Node) hasAddrAndID(addr Addr, id int160.T) bool {
	return id == n.Id && n.Addr.String() == addr.String()
}

func (n *Node) IsSecure() bool {
	return NodeIdSecure(n.Id.AsByteArray(), n.Addr.IP())
}

func (n *Node) idString() string {
	return n.Id.ByteString()
}

func (n *Node) NodeInfo() (ret krpc.NodeInfo) {
	ret.Addr = n.Addr.KRPC()
	if n := copy(ret.ID[:], n.idString()); n != 20 {
		panic(n)
	}
	return
}

// Per the spec in BEP 5.
func (s *Server) IsGood(n *Node) bool {
	if s.nodeIsBad(n) {
		return false
	}
	return time.Since(n.lastGotResponse) < 15*time.Minute ||
		!n.lastGotResponse.IsZero() && time.Since(n.lastGotQuery) < 15*time.Minute
}
