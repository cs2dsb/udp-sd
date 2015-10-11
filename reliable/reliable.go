package reliable

import (
	"fmt"
	"sync"
	"math"
	"net"
	"time"
	"golang.org/x/net/ipv4"
	"github.com/Sirupsen/logrus"
	"github.com/cs2dsb/udp-sd/util"
)

const (
	keepalive_timeout = time.Millisecond * 500
	default_retries = 5	
	default_packet_timeout = time.Millisecond * 2000
	default_connection_timeout = time.Millisecond * 10000
	incoming_buffer_length = 64*1024	
	tight_loop_delay = time.Millisecond * 1		
	
	rtt_packet_history = 10
	rtt_thresh_high = time.Millisecond * 800
	rtt_thresh_low = time.Millisecond * 250
	rtt_thresh_check_min_packets = 10
	
	dispatch_quality_levels = 15
	dispatch_quality_default_level = 3
	dispatch_quality_max = 20
	dispatch_quality_min = 2000
	
	dispatch_quality_poll_delay = dispatch_quality_max / 2
)

var (
	dispatch_levels []time.Duration
)

func init() {
	dispatch_levels = make([]time.Duration, dispatch_quality_levels)
	exponent := (math.Log(dispatch_quality_min) / math.Log(dispatch_quality_max) - 1.0) / (dispatch_quality_levels - 1.0)
	for i := 0; i < dispatch_quality_levels; i++ {
		dispatch_levels[i] = time.Millisecond * time.Duration(math.Pow(dispatch_quality_max, 1 + (exponent * float64(i))))
	}
	fmt.Printf("Dispatch levels: %v\n", dispatch_levels)
}

type con struct {
	done chan struct{}	
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
	sweepInterval time.Duration
}

type Connection interface {
	sendPacket(p *packet)
	sendKeepaliveToPeer(p *peer)
	getConnectionTimeout() time.Duration
	getPacketTimeout() time.Duration
	FindOrAddPeer(address *net.UDPAddr) *peer
	GetPeerList() []*peer
	GetListenAddresses() []*net.UDPAddr
	GetIncomingPacketChannel() chan *packet
	IsOnline() bool
	GetSweepInterval() time.Duration
}

func (c *con) GetSweepInterval() time.Duration {
	return c.sweepInterval
}

func (c *con) IsOnline() bool {	
	select {
		case <- c.done:
			return false
		default:
			return true
	}
}

func (c *con) GetListenAddresses() (addresses []*net.UDPAddr) {			
	addresses = make([]*net.UDPAddr, 0)
	
	
	select {
		case <- c.done:
			return
		default:
	}
		
	port := c.Port
	
	interfaces := util.GetUnicastInterfaces()
	ips := make([]net.IP, 0)	
	for _, ifc := range interfaces {
		addrs, err := ifc.Addrs()
		if err != nil {
			c.Warnf("Error getting addresses from interface (%v): %v", ifc, err)
			continue
		}
		for _, a := range addrs {
			var ip net.IP
        	switch v := a.(type) {
		        case *net.IPNet:
	                ip = v.IP
        		case *net.IPAddr:
        	        ip = v.IP
				default:
					continue
	        }
			
			
			if ip.To4() == nil {
				continue
			}
			found := false
			for _, oldA := range ips {
				if ip.Equal(oldA) {
					found = true
					break
				}
			}
			if !found {
				ips = append(ips, ip)
			}
		}
	}
	
	for _, ip := range ips {
		addresses = append(addresses, &net.UDPAddr{ IP: ip, Port: port })
	}
	
	return
}

func (c *con) GetIncomingPacketChannel() chan *packet {
	return c.IncomingPackets
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
	
	go func() {
		defer c.Infof("socket listener shutting down")
		defer socket.Close()
		defer close(incomingPackets)
		
		c.connection = ipv4.NewPacketConn(socket)
		c.Port = c.listenAddress.Port
		
		wg.Done()
		
		incomingBuf := make([]byte, incoming_buffer_length)
		for {		
			select {
				case <- c.done:
					return
				default:
			}
				
			time.Sleep(tight_loop_delay)			
			n, _, src, err := c.connection.ReadFrom(incomingBuf)
			
			if err != nil {		
				select {
					case <- c.done:
						return
					default:
						c.Errorf("Error reading from connection: %q", err)
						continue
				}
			}
			
			peer := c.FindOrAddPeer(src.(*net.UDPAddr))
			if peer == nil {
						
				select {
					case <- c.done:
						return
					default:
						c.Errorf("Nil peer was returned but we're still online", err)
						continue
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
		for {
			select {
				case <- c.done:
					return
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
		for {
			select {
				case <- c.done:
					for _, op := range peers {
						op.Close()
						c.Infof("Connection is offline, removing peer: %v", op)
					}
					return
				case p := <- c.removePeer:
					for i, op := range peers {
						if p.Address.IP.Equal(op.Address.IP) && p.Address.Port == op.Address.Port {
							op.Close()
							c.Infof("Removing peer: %v", op)
							peers = append(peers[:i], peers[i+1:]...)
							continue
						}
					}
			}
		}
	}()
	
	go func() {
		for {
			select {
				case <- c.done:
					return
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
			close(req.RespChan)
		case c.enumeratePeers <- req:
	}
	
	return req
}

func (c *con) GetPeerList() []*peer {
	peers := make([]*peer, 0)
	for p := range c.getEnumeratePeersChan().RespChan {
		peers = append(peers, p)
	}
	return peers
}

func (c *con) sweepPackets() {
	defer close(c.FailedOutgoingPackets)
	for {		
		select {
			case <- c.done:
				return
			default:				
		}	
						
		time.Sleep(c.packetTimeout)
		now := time.Now()
		
		enumReq := c.getEnumeratePeersChan()
		for p := range enumReq.RespChan {
			if now.After(p.lastAck.Add(p.connectionTimeout)) {
				c.Infof("%v idle, removing", p)				
				c.RemovePeer(p)
				continue
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
	//delay := now.Sub(p.timestamp)
	
//	c.Infof("Packet sat in queue for %v", delay)
	
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
	//c.Infof("Sent packet %v", p)	
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
	tick := time.NewTicker(dispatch_quality_poll_delay)
	
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
}

func (c *con) handlePackets(incomingPackets chan *encodedPacket) {
	defer close(c.IncomingPackets)
	for p := range incomingPackets {
		pkt, err := p.toOutgoingPacket()
		if err != nil {
			c.Errorf("Error reassembling packet from incoming bytes: %q", err)
			continue
		}
		
		pkt.Peer.ReceivePacket(pkt)
		
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

func (c *con) FindOrAddPeer(address *net.UDPAddr) *peer {
	resp := make(chan *peer)
		
	select {
		case <- c.done:
			return nil
		case c.findOrAddPeer <- &findPeerRequest{
				Address: address,
				RespChan: resp,
			}:
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
	pkt.Peer.SendPacket(pkt)	
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
	//c.Infof("Sending keepalive to %v", p)
	pkt := newPacketWithRetries(p, nil, 0, opPing)
	c.queuePacketForSend(pkt)	
}

func (c *con) EnableKeepalive(on bool) {
	c.keepalive = on
}

func (c *con) Close() {
	
	select {
		case <- c.done:
			return
		default:
			close(c.done)
	}
	c.Infof("Reliable connection (%v) closing", c)
	c.connection.SetReadDeadline(time.Now())
}

func newReliableConnectionWithListenerHandlers(c *con, handlePackets func(incomingPackets chan *encodedPacket), dispatchPackets func()) (*con, error) {		
	c.done = make(chan struct{})
	c.keepalive = true
	c.packetTimeout = default_packet_timeout
	c.connectionTimeout = default_connection_timeout	
	c.sweepInterval = default_packet_timeout
		
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