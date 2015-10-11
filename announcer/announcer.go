package announcer

import (
	"fmt"
	"time"
	"net"
	"encoding/binary"
	"strconv"
	"github.com/cs2dsb/udp-sd/reliable"
	"github.com/cs2dsb/udp-sd/util"
	"golang.org/x/net/ipv4"
	"github.com/satori/go.uuid"
	log "github.com/Sirupsen/logrus"
)

const (
	announce_group = "239.255.215.92"
	announce_port = "4568"
	protocol_id_size = 4
	protocol_id = uint32(485636284)	

	announce_interval = time.Millisecond * 1000
	announce_peer_count = 3
	announce_peer_list_max = 3
)

type announcer struct {
	unicastConnection reliable.Connection
	listening bool
	announceAddress *net.UDPAddr
	peers *peerList
	id uuid.UUID
}

func NewAnnouncer(servicesOffered []string, dedicated bool) (*announcer, error) {
	con, err := reliable.NewReliableConnection()
	if err != nil {
		return nil, err
	}
	
	
	annGroup := net.ParseIP(announce_group)
	annPort64, _ := strconv.ParseInt(announce_port, 10, 64)
	annPort := int(annPort64)
	annAddress := &net.UDPAddr{IP: annGroup, Port: annPort}
	
	myUuid := uuid.NewV1()
	
	a := &announcer{
		unicastConnection: con,
		announceAddress: annAddress,
		id: myUuid,
		peers: NewPeerList(),
	}
	
	a.peers.addOrUpdateAnnouncePeer(&announcePeer {
		Id: myUuid,
		Addresses: con.GetListenAddresses(),
		ServicesOffered: servicesOffered,
	})
	
	go a.listenToUnicast()
	go a.periodicAnnounce()
	
	if dedicated {
		err = a.Listen()
		if err != nil {
			return nil, err
		}
	}
	
	return a, nil
}

func checkProtocolId(buf []byte) bool {
	if len(buf) < protocol_id_size { 
		return false
	}
	p := binary.BigEndian.Uint32(buf)
	return p == protocol_id
}

func addProtocolId(buf []byte) error {
	if len(buf) < protocol_id_size {
		return fmt.Errorf("buf must be at least %d long to add protocolId, got %d", protocol_id_size, len(buf))
	}
	binary.BigEndian.PutUint32(buf, protocol_id)	
	return nil
}

func (a *announcer) periodicAnnounce() {	
	for a.unicastConnection.IsOnline() {
		time.Sleep(announce_interval)
		
		a.runAnnounceRound()
	}
}

func (a *announcer) runAnnounceRound() error {
	myPeer := announcePeerList {
		a.peers.getPeer(a.id),
	}
	
	targetPeers := a.peers.getRandomPeers(announce_peer_count, myPeer, peerHasAddress)
	if len(targetPeers) == 0 {
		return fmt.Errorf("Don't have any peers to advertise to")
	}
	
	for _, target := range targetPeers {
		announcePeers := a.peers.getRandomPeers(announce_peer_list_max, announcePeerList { target }, peerHasId)
		if len(announcePeers) == 0 {
			return fmt.Errorf("Don't have anything to advertise to peers")
		}
		
		payload, err := announcePeers.toBytes()
		if err != nil {
			return err
		}
		
		for _, addr := range target.Addresses {
			udpPeer := a.unicastConnection.FindOrAddPeer(addr)
			log.Infof("%v: Sending (%v) to (%v)", a.unicastConnection, announcePeers, addr)
			a.announceToPeer(udpPeer, payload)
		}		
	}
	return nil
}

func (a *announcer) Listen() error {
	if a.listening {
		return nil
	}
	
	interfaces := util.GetMulticastInterfaces()
	annSocket, err := net.ListenUDP("udp4", a.announceAddress)
	if err != nil {
		return err
	}
	
	annConn := ipv4.NewPacketConn(annSocket)
	
	for _, ifc := range interfaces {
		err = annConn.JoinGroup(ifc, a.announceAddress)
		if err != nil {
			return err
		}
	}
	
	go func() {
		defer annConn.Close()
		
		incomingBuf := make([]byte, 64*1024)
		
		for a.listening {
			n, _, src, err := annConn.ReadFrom(incomingBuf)
			if err != nil {
				log.Errorf("Error reading from connection (%v): %v", annConn, err)
				time.Sleep(time.Millisecond * 100)
				continue
			}
			
			if !checkProtocolId(incomingBuf[:n]) {
				continue
			}
			
			a.addPeer(src.(*net.UDPAddr))
		}
	}()
	
	return nil
}

func (a *announcer) listenToUnicast() {
	for p := range a.unicastConnection.GetIncomingPacketChannel() {
		a.handlePacket(p)
	}
}

func (a *announcer) addPeer(address *net.UDPAddr) {
	peer := NewAnnouncePeer(address)
	a.peers.addOrUpdateAnnouncePeer(peer)
}

func (a *announcer) announceToPeer(p reliable.Peer, payload []byte) {
	if len(payload) == 0 {
		return
	}
	
	p.SendData(payload)
}

func (a *announcer) handlePacket(p reliable.Packet) {
	if !p.IsData() {
		return
	}
	
	payload := p.GetPayload()
	if !checkProtocolId(payload) {
		return
	}
	
	peers, err := announcePeerListFromBytes(payload[protocol_id_size:])
	if err != nil {
		log.Errorf("Can't handle announce packet, peerListFromBytes returned error: %v", err)
		return
	}

	for _, peer := range peers {		
		log.Infof("Adding peer from announcement: %v", peer)	
		a.peers.addOrUpdateAnnouncePeer(peer)
	}
}
















