package peer_store

import (
	"testTorrent/dht/krpc"
	"testTorrent/torrent/metainfo"
)

type InfoHash = metainfo.Hash

type Interface interface {
	AddPeer(InfoHash, krpc.NodeAddr)
	GetPeers(InfoHash) []krpc.NodeAddr
}
