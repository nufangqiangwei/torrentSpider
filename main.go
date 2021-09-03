package main

import (
	"fmt"
	"golang.org/x/xerrors"
	"net"
	"testTorrent/dht"
	"testTorrent/dht/int160"
	"testTorrent/torrent"
	"testTorrent/torrent/metainfo"
	"testTorrent/torrent/tracker"
	"testTorrent/www"
	"time"
)

func main() {
	//pingNode("tracker4.itzmx.com:2710")
	//www.GetUser()
	aa := NewTimeWheel(&TimeWheelConfig{})
	println(aa.wheel)
	www.GetUser()
}

func NewTorrentClient() (client *torrent.Client) {
	clientConfig := torrent.NewDefaultClientConfig()
	client, err := torrent.NewClient(clientConfig)
	if err != nil {
		panic(fmt.Sprintf("creating client: %v", err))
	}
	//defer client.Close()
	return
}
func TestUdpServer() *dht.Server {
	ret, err := net.ListenPacket("udp", ":5000")
	if err != nil {
		panic(err)
	}
	StrInfoHash := []byte("64a980abe6e448226bb930ba061592e44c3781a1")
	NodeId := [20]byte{}
	copy(NodeId[:19], StrInfoHash)
	UdpServer, err := dht.NewServer(&dht.ServerConfig{
		NodeId:        NodeId,
		Conn:          ret,
		NoSecurity:    true,
		StartingNodes: func() ([]dht.Addr, error) { return dht.GlobalBootstrapAddrs("udp") },
	})
	if err != nil {
		panic(err)
	}

	//defer UdpServer.Close()
	//tl, _ := UdpServer.Bootstrap()
	//fmt.Printf("%+v\n", tl)
	return UdpServer
}

func pingNode(nodeAddres string) {
	if nodeAddres == "" {
		nodeAddres = "router.bittorrent.com:6881"
	}
	a, err := net.ResolveUDPAddr("udp", nodeAddres)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("%+v\n", *a)
	udps := TestUdpServer()

	fmt.Printf("%+v\n", udps.Ping(a))
}
func ParsingTorrentFile(client *torrent.Client, fileName string) (*torrent.Torrent, error) {
	if fileName == "" {
		fileName = "ubuntu-21.04-desktop-amd64.iso.torrent"
	}
	metaInfo, err := metainfo.LoadFromFile(fileName)
	if err != nil {
		return nil, xerrors.Errorf("error loading torrent file %q: %s\n", fileName, err)
	}
	t, err := client.AddTorrent(metaInfo)
	if err != nil {
		return nil, xerrors.Errorf("adding torrent: %w", err)
	}
	return t, nil
}

func TrackerGetPeers() {
	StrInfoHash := []byte("64a980abe6e448226bb930ba061592e44c3781a1")
	infoHash := [20]byte{}
	copy(infoHash[:19], StrInfoHash)
	//"https://torrent.ubuntu.com/announce"
	response, err := tracker.Announce{
		TrackerUrl: "https://tr.torland.ga:443/announce",
		Request: tracker.AnnounceRequest{
			InfoHash: infoHash,
			Port:     uint16(torrent.NewDefaultClientConfig().ListenPort),
		},
	}.Do()
	if err != nil {
		fmt.Printf("doing announce: %s", err.Error())
		return
	}
	fmt.Printf("%+v\n", response)
}

/*
记录所有请求过来的node节点数据
对get_peer的请求记录种子信息，后续对外请求
定期储存种子信息
*/
type CommunicationRecord struct {
	// 时间段 一个小时 记录一个小时中有多少次请求交互 其中有我主动发送的数量，他主动发送的数量
	// 数据格式 每个小时站6个字节，前2个字节 我主动发送的数量 然后他回复的数量，他发送的数量
	// 如果联系多个小时交互数据都相同就压缩
}
type nodeInfo struct {
	nodeData            dht.Node
	ReceivedTime        int64
	communicationRecord CommunicationRecord
}

var nodeMap map[int160.T]nodeInfo

func addNewNode(node dht.Node) {
	a, ok := nodeMap[node.Id]
	if !ok {
		a = nodeInfo{
			nodeData:     node,
			ReceivedTime: time.Now().Unix(),
		}
	}
	a.communicationRecord.add()
}
func (cr *CommunicationRecord) add() {

}
