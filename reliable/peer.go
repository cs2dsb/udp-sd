package reliable

import (
	"fmt"
	"net"
	"time"
	log "github.com/Sirupsen/logrus"
)

type peer struct {
	Address *net.UDPAddr
	lastAck time.Time
	Seq uint32
	RemoteSeq uint32
	AckBitfield uint32
	UnAckedPackets map[uint32]*packet
	packetTimeout time.Duration
	connectionTimeout time.Duration
	connection conInterface
	dispatchLevel int
	outgoingPackets chan *packet
	closed bool
	done chan struct{}
	lastDispatch time.Time
}

func NewPeer(address *net.UDPAddr, c conInterface) *peer {
	p := &peer{}
	p.Address = address
	p.Seq = 1
	p.UnAckedPackets = make(map[uint32]*packet)
	p.connectionTimeout = c.getConnectionTimeout()
	p.packetTimeout = c.getPacketTimeout()
	p.connection = c
	p.outgoingPackets = make(chan *packet)
	p.done = make(chan struct{})
	p.lastAck = time.Now().Add(-keepalive_timeout)
	return p
}

func (p *peer) Disconnect() {
	if !p.closed {
		p.closed = true
		close(p.done)
	}
}

func (p *peer) String() string {
	if p.Address != nil {
		return fmt.Sprintf("Peer{%v:%d}", p.Address.IP, p.Address.Port)
	} 
	return "Peer{NOADDRESS}"
}

func (p *peer) isAlive() bool {
	return p.RemoteSeq > 0 && p.lastAck.Add(p.packetTimeout).After(time.Now())
}
func (p *peer) IsAlive(wait time.Duration) bool {
	if p.isAlive() {
		return true
	}
	w := time.Millisecond * 200
	if wait < w {
		w = wait
	}
	for wait > 0 {
		time.Sleep(w)
		wait -= w
		
		ok := p.isAlive()
		if ok {
			return true
		}
	}
	
	return p.isAlive()
}

func (p *peer) waitForAck(seq uint32, wait time.Duration) bool {
	_, ok := p.UnAckedPackets[seq]
	if !ok {
		return true
	}
	
	w := time.Millisecond * 200
	if wait < w {
		w = wait
	}
	for wait > 0 {
		time.Sleep(w)
		wait -= w
		
		_, ok = p.UnAckedPackets[seq]
		if !ok {
			return true
		}	
	}
	
	_, ok = p.UnAckedPackets[seq]
	return !ok
}


func (p *peer) updateRemoteSeq(seq uint32) {
	diff := seq - p.RemoteSeq
	gt := true
	if seq < p.RemoteSeq {
		diff = p.RemoteSeq - seq
		gt = false
	}
	if diff == 0 {
		log.Warningf("Dropping duplicate packet %d for %v", seq, p)
	} else if (gt) {		
		if p.RemoteSeq == 0 {
			diff = 0
		}
		
		p.RemoteSeq = seq
		
		if diff > 0 {
			p.AckBitfield = p.AckBitfield << diff
			p.AckBitfield |= 1 << (diff - 1)
		}
	} else {
		p.AckBitfield |= 1 << (diff - 1)
	}
}

func (p *peer) ackPacket(ack uint32) {
	_, ok := p.UnAckedPackets[ack]
	if ok {
		log.Infof("%v Removing unacked packet with seq %d", p, ack)
		delete(p.UnAckedPackets, ack)
	}
}

func (p *peer) addUnAckedPacket(pkt *packet) {
	_, ok := p.UnAckedPackets[pkt.Seq]
	if ok {
		log.Warningf("Found a packet with seq %d when trying to add an unacked packet with that seq", pkt.Seq)
	}
	p.UnAckedPackets[pkt.Seq] = pkt
}

func (p *peer) SendData(payload []byte) {
	packet := newPacket(p, payload, opData)
	p.queuePacketForSend(packet)
}

func (p *peer) queuePacketForSend(pkt *packet) {
	if pkt == nil {
		log.Warningf("peer.queuePacketForSend got passed a nil packet")
		return
	}
	
	pkt.timestamp = time.Now()
	
	pkt.Seq = p.Seq
	p.Seq++
	
	if pkt.OpCode & opAck != opAck {
		p.addUnAckedPacket(pkt)	
	}
	
	select {
		case <- p.done:
		case p.outgoingPackets <- pkt:
	}	
}

func (p *peer) dispatchInterval() time.Duration {
	return time.Millisecond * time.Duration(dispatch_queue_quality_max + p.dispatchLevel * dispatch_queue_quality_step)
}

func (p *peer) dispatch(now time.Time) {	
	if p.lastDispatch.Add(p.dispatchInterval()).After(now) {
		return
	}
	
	var pkt *packet
	select {
		case pkt = <- p.outgoingPackets:
		default:		
			return
	}
	
	if pkt == nil {
		return
	}
	
	p.lastDispatch = now
	p.connection.sendPacket(pkt)
}

func (p *peer) keepalive(now time.Time) {
	if p.lastDispatch.Add(keepalive_timeout).After(now) {
		return
	}
	p.connection.sendKeepaliveToPeer(p)
}














