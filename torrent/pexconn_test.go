package torrent

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"testTorrent/dht/krpc"

	"testTorrent/torrent/metainfo"
	pp "testTorrent/torrent/peer_protocol"
)

func TestPexConnState(t *testing.T) {
	cl := Client{
		config: TestingConfig(t),
	}
	cl.initLogger()
	torrent := cl.newTorrent(metainfo.Hash{}, nil)
	addr := &net.TCPAddr{IP: net.IPv6loopback, Port: 4747}
	c := cl.newConnection(nil, false, addr, addr.Network(), "")
	c.PeerExtensionIDs = make(map[pp.ExtensionName]pp.ExtensionNumber)
	c.PeerExtensionIDs[pp.ExtensionNamePex] = pexExtendedId
	c.messageWriter.mu.Lock()
	c.setTorrent(torrent)
	torrent.addPeerConn(c)

	c.pex.Init(c)
	require.True(t, c.pex.IsEnabled(), "should get enabled")
	defer c.pex.Close()

	var out pp.Message
	writerCalled := false
	testWriter := func(m pp.Message) bool {
		writerCalled = true
		out = m
		return true
	}
	<-c.messageWriter.writeCond.Signaled()
	c.pex.Share(testWriter)
	require.True(t, writerCalled)
	require.EqualValues(t, pp.Extended, out.Type)
	require.EqualValues(t, pexExtendedId, out.ExtendedID)

	x, err := pp.LoadPexMsg(out.ExtendedPayload)
	require.NoError(t, err)
	targx := pp.PexMsg{
		Added:      krpc.CompactIPv4NodeAddrs(nil),
		AddedFlags: []pp.PexPeerFlags{},
		Added6: krpc.CompactIPv6NodeAddrs{
			mustNodeAddr(addr),
		},
		Added6Flags: []pp.PexPeerFlags{0},
	}
	require.EqualValues(t, targx, x)
}
