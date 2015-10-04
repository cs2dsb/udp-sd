package reliable

import (
	"sync"
	"net"
	"time"
	"encoding/binary"
	"golang.org/x/net/ipv4"
	log "github.com/Sirupsen/logrus"
)

const (
	outgoing_channel_length = 10
	keepalive_timeout = time.Second * 10
	default_retries = 5
	protocol_id = 234545346
	default_packet_timeout = time.Millisecond * 800
	default_connection_timeout = time.Millisecond * 10000
	incoming_buffer_length = 64*1024	
	tight_loop_delay = time.Millisecond * 100		
)

var interfaces []*net.Interface
var protocolIdBytes []byte

type con struct {
	online bool	
	Port int
	connection *ipv4.PacketConn
	interfaces []*net.Interface	
	keepalive bool
	outgoingPackets chan *packet
	peers []*peer
	connectionTimeout time.Duration
	packetTimeout time.Duration
	IncomingPackets chan *packet
}

func getProtocolIdBytes() *[]byte {
	if len(protocolIdBytes) == 0 {
		pid := uint32(protocol_id)
		protocolIdBytes = make([]byte, binary.Size(pid))
		binary.PutUvarint(protocolIdBytes, uint64(pid))
	}
	return &protocolIdBytes
}

func uint32ToBytes(num uint32) []byte {
	buf := make([]byte, binary.Size(num))
	binary.PutUvarint(buf, uint64(num))
	return buf
}

func uint32FromBytes(buf []byte) (uint32, int) {
	var r uint32
	n := binary.Size(r)
	r64, _ := binary.Uvarint(buf[0:n])
	r = uint32(r64)
	return r, n
}

func (c *con) startListners() error {
	socket, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		log.Errorf("Error establishing packet socket: %v", err)
		return err
	}

	var wg sync.WaitGroup 
	wg.Add(1)
	
	incomingPackets := make(chan *encodedPacket)
	c.outgoingPackets = make(chan *packet, outgoing_channel_length)
	c.IncomingPackets = make(chan *packet)
	
	go func() {
		defer socket.Close()
		defer close(incomingPackets)
		defer close(c.outgoingPackets)
		
		c.connection = ipv4.NewPacketConn(socket)
		c.Port = socket.LocalAddr().(*net.UDPAddr).Port
		c.online = true
		
		wg.Done()
		
		incomingBuf := make([]byte, incoming_buffer_length)
		for c.online {
			time.Sleep(tight_loop_delay)
			
			n, _, src, err := c.connection.ReadFrom(incomingBuf)
			if err != nil {
				log.Errorf("Error reading from connection: %q", err)
				continue
			}
			
			buf := make([]byte, n)
			peer := c.FindOrAddPeer(src.(*net.UDPAddr))
			pkt := encodedPacket {
				Peer: peer,
				Payload: &buf,
			}		
			copy(buf, incomingBuf[0:n])
			
			incomingPackets <- &pkt
		}
	}()
	
	wg.Wait()
	
	go c.handlePackets(incomingPackets)
	go c.dispatchPackets()	
	go c.sweepPackets()
	
	return nil
}

func (c *con) sweepPackets() {
	for c.online {
		time.Sleep(c.packetTimeout)
		now := time.Now()
		
		for pi := len(c.peers) - 1; pi >= 0; pi -- {
			p := c.peers[pi]
			
			if time.Now().After(p.lastAck.Add(p.connectionTimeout)) {
				log.Infof("%v idle, removing", p)
				c.peers = append(c.peers[:pi], c.peers[pi+1:]...)
				continue
			}
			
			for seq, pkt := range p.UnAckedPackets {
				if now.After(pkt.timestamp.Add(p.packetTimeout)) {
					continue
				}
				
				delete(p.UnAckedPackets, seq)
				
				if pkt.retries > 0 {
					log.Infof("Retrying %d (%d left)", seq, pkt.retries)
					pkt.retries--					
					c.queuePacketForSend(pkt)
				} else {
					log.Infof("Dropping unacked packet %d", seq)
				}
			}
		}
	}
}

func (c *con) sendPacket(p *packet) {	
	if p.Peer == nil {
		log.Error("Packet with no peer passed to con.sendPacket")
		return
	}

	//If it has retries or is a ping is must be reliable	
	if p.retries > 0 || (p.OpCode & opPing == opPing) {
		p.OpCode |= opReliable		
	}
	//If it's an ack that's all it can be
	if p.OpCode & opAck == opAck {
		p.OpCode = opAck
	}
	
	p.AckBitfield = p.Peer.AckBitfield
	
	buf := p.toBytes()
	if buf == nil {
		log.Error("outgoingPacket.ToBytes() returned nil in con.sendPacket, nothing to send")
		return
	}
	
	c.connection.SetTOS(0x0)
	c.connection.SetTTL(16)
	_, err := c.connection.WriteTo(*buf, nil, p.Peer.Address)
	if err != nil {
		log.Errorf("Error writing bytes to connection: %q", err)
		//Do retry
		return
	}
	log.Infof("Sent packet to %v", p.Peer)	
}

func (c *con) dispatchPackets() {
	var lastSend time.Time //Zero so if keepalive is on it starts right away
	tick := time.NewTicker(time.Millisecond * 500)
	
	for {
		select {
			case p, ok := <- c.outgoingPackets:
				if !ok {
					log.Infof("Outgoing packet channel closed, handler function exiting")
					return
				}
				lastSend = time.Now()
				c.sendPacket(p)	
			case <- tick.C:			
				if c.keepalive && time.Now().After(lastSend.Add(keepalive_timeout)) {
					lastSend = time.Now()
					c.sendKeepalive()
				}
		}	
		
	}
}

func (c *con) handlePackets(incomingPackets chan *encodedPacket) {
	for p := range incomingPackets {
		log.Infof("Got packet from %v", p.Peer)
		
		pkt, err := p.toOutgoingPacket()
		if err != nil {
			log.Errorf("Error reassembling packet from incoming bytes: %q", err)
			continue
		}
		
		log.Infof("Reassembled packet: %v", pkt)
		c.ackIfNecessary(pkt)
		c.updatePeerWithPacket(pkt.Peer, pkt)
		
		select {
			case c.IncomingPackets <- pkt:
			default:
				log.Warnf("No one listening on IncomingPackets channel, throwing packets away")
		}		
	}
	log.Infof("Incoming packet channel closed, handler function exiting")
	close(c.IncomingPackets)
}

func (c *con) FindOrAddPeer(address *net.UDPAddr) *peer {
	for _, p := range c.peers {
		if p.Address.IP.Equal(address.IP) && p.Address.Port == address.Port {
			return p
		}
	}
	
	return c.ConnectPeer(address)	
}

func (c *con) updatePeerWithPacket(peer *peer, pkt *packet) {
	if pkt.Seq == 8 { return }
	peer.lastAck = time.Now()
	
	acks := pkt.ackList()
	for _, a := range acks {
		peer.ackPacket(a)
	}
	
	peer.updateRemoteSeq(pkt.Seq)
}


func waitUntilTrue(test func() bool, wait time.Duration) bool {
	w := time.Millisecond * 200
	for wait > 0 {
		if test() {
			return true
		}
		wait -= w
		time.Sleep(w)
	}
	return test()
}

func (c *con) ackIfNecessary(p *packet) {
	if p.OpCode & opReliable == opReliable {		
		log.Infof("Acking packet")
		pkt := newPacketWithRetries(p.Peer, nil, 0, opAck)
		pkt.Ack = p.Seq
		c.queuePacketForSend(pkt)
	}
}

func (c *con) addPeer(p *peer) {	
	for _, op := range c.peers {
		if p.Address.IP.Equal(op.Address.IP) && p.Address.Port == op.Address.Port {
			log.Infof("Peer at %v:%d already exists", p.Address.IP, p.Address.Port)
			return
		}
	}
	
	log.Infof("Added a new peer at %v:%d", p.Address.IP, p.Address.Port)
	c.peers = append(c.peers, p)
	c.sendKeepaliveToPeer(p)	
}

func (c *con) ConnectPeer(address *net.UDPAddr) *peer {
	p := &peer{}
	p.Address = address
	p.Seq = 1
	p.UnAckedPackets = make(map[uint32]*packet)
	p.connectionTimeout = c.connectionTimeout
	p.packetTimeout = c.packetTimeout
	p.connection = c
	c.addPeer(p)	
	return p
}

func (c *con) queuePacketForSend(pkt *packet) {
	pkt.timestamp = time.Now()
	
	peer := pkt.Peer
	pkt.Seq = peer.Seq
	peer.Seq++
	
	peer.addUnAckedPacket(pkt)	
	
	if c.online {
		c.outgoingPackets <- pkt
	}
}

func (c *con) broadCastWithRetries(payload []byte, retries int, opCode opCode) {
	pkt := newPacketWithRetries(nil, payload, retries, opCode)
	
	for _, p := range c.peers {
		pkt.Peer = p
		c.queuePacketForSend(pkt)
	}
} 

func (c *con) BroadCast(payload []byte) {
	c.broadCastWithRetries(payload, default_retries, opData)
}

func (c *con) sendKeepalive() {
	log.Infof("Sending a keepalive to %d peers", len(c.peers))
	for _, p := range c.peers {
		c.sendKeepaliveToPeer(p)
	}
}

func (c *con) sendKeepaliveToPeer(p *peer) {
	pkt := newPacketWithRetries(p, nil, 0, opPing)
	c.queuePacketForSend(pkt)	
}

func (c *con) EnableKeepalive() {
	c.keepalive = true
}

func (c *con) Stop() {
	c.online = false
	c.connection.SetReadDeadline(time.Now())
}

func newReliableConnectionWithListenerHandlers(c *con, handlePackets func(incomingPackets chan *encodedPacket), dispatchPackets func()) (*con, error) {
	c.peers = make([]*peer, 0)
	c.packetTimeout = default_packet_timeout
	c.connectionTimeout = default_connection_timeout
		
	err := c.startListners()
	
	if err != nil {
		return nil, err
	}
	
	log.Infof("New reliable connection running on port %d", c.Port)
	
	return c, nil
}

func NewReliableConnection() (*con, error) {
	c := &con{}	
	return newReliableConnectionWithListenerHandlers(c, c.handlePackets, c.dispatchPackets)
}

func getViableInterfaces() []*net.Interface {
	if len(interfaces) == 0 {
		allInterfaces, err := net.Interfaces()
		if err != nil {
			panic(err)
		}
		
		interfaces := make([]*net.Interface, 0)
		
		for _, ifc:= range allInterfaces {
			if ifc.Flags & net.FlagUp == net.FlagUp && ifc.Flags & net.FlagMulticast == net.FlagMulticast {
				interfaces = append(interfaces, &ifc)
			}
		}
	}
	
	return interfaces
}