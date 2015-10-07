package reliable

import (
	"fmt"
	"sync"
	"net"
	"time"
	"golang.org/x/net/ipv4"
	"github.com/Sirupsen/logrus"
)

const (
	keepalive_timeout = time.Second * 5
	default_retries = 5	
	default_packet_timeout = time.Millisecond * 800
	default_connection_timeout = time.Millisecond * 10000
	incoming_buffer_length = 64*1024	
	tight_loop_delay = time.Millisecond * 100		
	
	rtt_packet_history = 10
	rtt_thresh_high = time.Millisecond * 800
	rtt_thresh_low = time.Millisecond * 250
	rtt_thresh_check_min_packets = 5
	
	dispatch_queue_quality_levels = 10
	dispatch_queue_quality_max = time.Millisecond * 1000 / 20
	dispatch_queue_quality_min = time.Millisecond * 1000
	dispatch_queue_quality_step = (dispatch_queue_quality_min - dispatch_queue_quality_max)/dispatch_queue_quality_levels
	dispatch_queue_quality_resolution = 24
)

func init() {
	fmt.Printf("Max dispatch: %v, Min dispatch: %v, Step: %v\n", dispatch_queue_quality_max, dispatch_queue_quality_min, dispatch_queue_quality_step)
}

var interfaces []*net.Interface

type con struct {
	done chan struct{}	
	online AtomicBool
	Port int
	connection *ipv4.PacketConn
	interfaces []*net.Interface	
	keepalive bool
	connectionTimeout time.Duration
	packetTimeout time.Duration
	IncomingPackets chan *packet
	FailedOutgoingPackets chan *packet
	findOrAddPeer chan *findPeerRequest
	removePeer chan *peer	
	enumeratePeers chan *enumeratePeersRequest
	listenAddress *net.UDPAddr
}

type conInterface interface {
	sendPacket(p *packet)
	sendKeepaliveToPeer(p *peer)
	getConnectionTimeout() time.Duration
	getPacketTimeout() time.Duration
}

func (c *con) getConnectionTimeout() time.Duration {
	return c.connectionTimeout
}

func (c *con) getPacketTimeout() time.Duration {
	return c.packetTimeout
}

func (c *con) String() string {
	return fmt.Sprintf("con{%v:%d}", c.listenAddress.IP, c.listenAddress.Port)
}

func (con *con) Errorf(fmt string, args ...interface{}) {
	logrus.WithField("reliable-connection", con).Errorf(fmt, args...)
}

func (con *con) Infof(fmt string, args ...interface{}) {
	logrus.WithField("reliable-connection", con).Infof(fmt, args...)
}

func (con *con) Warnf(fmt string, args ...interface{}) {
	logrus.WithField("reliable-connection", con).Warnf(fmt, args...)
}

type findPeerRequest struct {
	Address *net.UDPAddr
	RespChan chan *peer
}

type enumeratePeersRequest struct {
	RespChan chan *peer
	AbortChan chan struct{}
}

func (c *con) startListners(handlePackets func(incomingPackets chan *encodedPacket), dispatchPackets func()) error {
	socket, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		c.Errorf("Error establishing packet socket: %v", err)
		return err
	}
	
	c.listenAddress = socket.LocalAddr().(*net.UDPAddr)

	var wg sync.WaitGroup 
	wg.Add(1)
	
	incomingPackets := make(chan *encodedPacket)
	c.IncomingPackets = make(chan *packet)
	c.FailedOutgoingPackets = make(chan *packet)
	c.removePeer = make(chan *peer)
	c.enumeratePeers = make(chan *enumeratePeersRequest)
	c.findOrAddPeer = make(chan *findPeerRequest)
	c.done = make(chan struct{})
	
	go func() {
		defer socket.Close()
		defer close(incomingPackets)
		
		c.connection = ipv4.NewPacketConn(socket)
		c.Port = c.listenAddress.Port
		
		wg.Done()
		
		incomingBuf := make([]byte, incoming_buffer_length)
		for c.online.Get() {
			time.Sleep(tight_loop_delay)			
			n, _, src, err := c.connection.ReadFrom(incomingBuf)
			
			if err != nil {
				if c.online.Get() {
					c.Errorf("Error reading from connection: %q", err)
					continue
				} else {
					break
				}
			}
			
			peer := c.FindOrAddPeer(src.(*net.UDPAddr))
			if peer == nil {
				if c.online.Get() {
					c.Errorf("Nil peer was returned but we're still online", err)
					continue
				} else {
					break
				}
			}
			
			buf := make([]byte, n)
			pkt := encodedPacket {
				Peer: peer,
				Payload: buf,
			}
					
			copy(buf, incomingBuf[0:n])
			
			incomingPackets <- &pkt
		}
	}()
	
	wg.Wait()
	
	go handlePackets(incomingPackets)
	go dispatchPackets()	
	go c.sweepPackets()
	go c.peerManager()
	go c.sendKeepalives()
	
	return nil
}

func (c *con) peerManager() {
	peers := make([]*peer, 0)
			
	go func() {
		for c.online.Get() {
			select {
				case <- c.done:
				case pa := <- c.findOrAddPeer:
					found := false				
					for _, op := range peers {
						if pa.Address.IP.Equal(op.Address.IP) && pa.Address.Port == op.Address.Port {
							pa.RespChan <- op
							found = true
							break
						}
					}
					if !found {
						p := NewPeer(pa.Address, c)
						peers = append(peers, p)
						c.Infof("Adding peer: %v", p)	
						pa.RespChan <- p
					}
					close(pa.RespChan)
			}
		}
	}()
	
	go func() {
		for c.online.Get() {
			select {
				case <- c.done:
				case p := <- c.removePeer:
					for i, op := range peers {
						if p.Address.IP.Equal(op.Address.IP) && p.Address.Port == op.Address.Port {
							op.Disconnect()
							c.Infof("Removing peer: %v", op)
							peers = append(peers[:i], peers[i+1:]...)
							continue
						}
					}
			}
		}
	}()
	
	go func() {
		for c.online.Get() {
			select {
				case <- c.done:
				case ep := <- c.enumeratePeers:
					//c.Infof("Enumerating peers: %v", c)
					func() {
						defer close(ep.RespChan)
						for _, p := range peers {
							//c.Infof("Enumerate sending peer: %v", p)
							select {
								case ep.RespChan <- p:
								case <- ep.AbortChan:							
									return
							}
						}
					}()
			}
		}
	}()
}

func (c *con) getEnumeratePeersChan() *enumeratePeersRequest {
	req := &enumeratePeersRequest {
		RespChan: make(chan *peer),
		AbortChan: make(chan struct{}),
	}
	
	select {
		case <- c.done:
		case c.enumeratePeers <- req:		
			return req
	}
	
	if !c.online.Get() {
		close(req.RespChan)
	}
	
	return req
}

func (c *con) sweepPackets() {
	defer close(c.FailedOutgoingPackets)
	for c.online.Get() {		
		time.Sleep(c.packetTimeout)
		now := time.Now()
		
		enumReq := c.getEnumeratePeersChan()
		for p := range enumReq.RespChan {
			if time.Now().After(p.lastAck.Add(p.connectionTimeout)) {
				c.Infof("%v idle, removing", p)				
				c.RemovePeer(p)
				continue
			}
			
			toQueue := make([]*packet, 0)
			
			for seq, pkt := range p.UnAckedPackets {
				if pkt.timestamp.Add(p.packetTimeout).After(now) {
					continue
				}
				
				delete(p.UnAckedPackets, seq)
				
				if pkt.Retries > pkt.RetriesUsed {
					pkt.RetriesUsed++	
					toQueue = append(toQueue, pkt)
				} else {
					c.Infof("Dropping unacked packet %v", pkt)
					select {
						case c.FailedOutgoingPackets <- pkt:
						default:
							c.Warnf("No one listening to FailedOutgoingPackets channel")
					}
				}
			}
			
			for _, pkt := range toQueue {
				seq := pkt.Seq
				c.queuePacketForSend(pkt)
				c.Infof("Retrying %d as %d (%d/%d)", seq, pkt.Seq, pkt.RetriesUsed, pkt.Retries)
			}
		}
	}
}

func (c *con) sendPacket(p *packet) {	
	if p.Peer == nil {
		c.Errorf("Packet with no peer passed to con.sendPacket")
		return
	}
	
	now := time.Now()
	delay := now.Sub(p.timestamp)
	c.Infof("Packet sat in queue for %v", delay)
	
	//Since we want RTT not sat in queue + RTT time
	p.timestamp = now

	//If it has retries or is a ping is must be reliable	
	if p.Retries > 0 || (p.OpCode & opPing == opPing) {
		p.OpCode |= opReliable		
	}
	//If it's an ack that's all it can be
	if p.OpCode & opAck == opAck {
		p.OpCode = opAck
	}
	
	p.AckBitfield = p.Peer.AckBitfield
	
	buf := p.toBytes()
	if buf == nil {
		c.Errorf("outgoingPacket.ToBytes() returned nil in con.sendPacket, nothing to send")
		return
	}
	
	c.connection.SetTOS(0x0)
	c.connection.SetTTL(16)
	_, err := c.connection.WriteTo(buf, nil, p.Peer.Address)
	if err != nil {
		c.Errorf("Error writing bytes to connection: %q", err)
		//Do retry
		return
	}
	c.Infof("Sent packet %v", p)	
}

func (c *con) sendKeepalives() {
	tick := time.NewTicker(time.Millisecond * 500)
	for {
		select {
			case <- c.done:
				return 
			default:
		}
		
		now := <- tick.C
		enumReq := c.getEnumeratePeersChan()
		for p := range enumReq.RespChan {
			p.keepalive(now)
		}
	}
}

func (c *con) dispatchPackets() {
	tick := time.NewTicker(dispatch_queue_quality_max)
	
	for {
		select {
			case <- c.done:
				return 
			case now := <- tick.C:		
				enumReq := c.getEnumeratePeersChan()
				for p := range enumReq.RespChan {
					p.dispatch(now)
				}
		}	
		
	}

	enumReq := c.getEnumeratePeersChan()
	for p := range enumReq.RespChan {
		c.removePeer <- p
	}
}

func (c *con) handlePackets(incomingPackets chan *encodedPacket) {
	defer close(c.IncomingPackets)
	for p := range incomingPackets {
		pkt, err := p.toOutgoingPacket()
		if err != nil {
			c.Errorf("Error reassembling packet from incoming bytes: %q", err)
			continue
		}
		
		c.Infof("Reassembled packet: %v", pkt)
		c.ackIfNecessary(pkt)
		c.updatePeerWithPacket(pkt.Peer, pkt)
		
		if pkt.OpCode & opData == opData {
			select {
				case c.IncomingPackets <- pkt:
				default:
					c.Warnf("No one listening on IncomingPackets channel, throwing packets away")
			}		
		}
	}
	c.Infof("Incoming packet channel closed, handler function exiting")
}

func (c *con) updatePeerWithPacket(peer *peer, pkt *packet) {
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
		c.Infof("Acking packet: %v", p)
		pkt := newPacketWithRetries(p.Peer, nil, 0, opAck)
		pkt.Ack = p.Seq
		c.queuePacketForSend(pkt)
	}
}

func (c *con) FindOrAddPeer(address *net.UDPAddr) *peer {
	resp := make(chan *peer)
	
	if !c.online.Get() {
		return nil
	}
	
	select {
		case <- c.done:
		case c.findOrAddPeer <- &findPeerRequest{
				Address: address,
				RespChan: resp,
			}:
	}
	
	if !c.online.Get() {
		return nil
	}
	
	p := <- resp
	return p
}

func (c *con) RemovePeer(p *peer) {
	select {
		case <- c.done:		
		case c.removePeer <- p:
	}
}

func (c *con) queuePacketForSend(pkt *packet) {
	pkt.Peer.queuePacketForSend(pkt)
}

func (c *con) broadCastWithRetries(payload []byte, retries uint32, opCode opCode) {
	pkt := newPacketWithRetries(nil, payload, retries, opCode)

	enumReq := c.getEnumeratePeersChan()
	for p := range enumReq.RespChan {		
		pkt.Peer = p
		c.queuePacketForSend(pkt)
	}
} 

func (c *con) BroadCast(payload []byte) {
	c.broadCastWithRetries(payload, default_retries, opData)
}

func (c *con) sendKeepalive() {
	enumReq := c.getEnumeratePeersChan()
	for p := range enumReq.RespChan {
		c.sendKeepaliveToPeer(p)
	}
}

func (c *con) sendKeepaliveToPeer(p *peer) {
	c.Infof("Sending keepalive to %v", p)
	pkt := newPacketWithRetries(p, nil, 0, opPing)
	c.queuePacketForSend(pkt)	
}

func (c *con) EnableKeepalive(on bool) {
	c.keepalive = on
}

func (c *con) Stop() {
	c.Infof("Reliable connection (%v) stopping", c)
	
	c.online.Set(false)
	//Aborts any sends to peer channels
	close(c.done)	
	
	c.connection.SetReadDeadline(time.Now())
}

func newReliableConnectionWithListenerHandlers(c *con, handlePackets func(incomingPackets chan *encodedPacket), dispatchPackets func()) (*con, error) {	
	c.online.Set(true)
	c.keepalive = true
	c.packetTimeout = default_packet_timeout
	c.connectionTimeout = default_connection_timeout	
		
	err := c.startListners(handlePackets, dispatchPackets)
	
	if err != nil {
		return nil, err
	}
	
	c.Infof("New reliable connection running on port %d", c.Port)	
	
	
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