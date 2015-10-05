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
	connection *con
}

func (p *peer) String() string {
	if p.Address != nil {
		return fmt.Sprintf("Peer{%v:%d}", p.Address.IP, p.Address.Port)
	} 
	return "Peer{NOADDRESS}"
}

func (p *peer) IsAlive(wait time.Duration) bool {
	if p.lastAck.Add(p.packetTimeout).After(time.Now()) {
		return true
	}
	w := time.Millisecond * 200
	if wait < w {
		w = wait
	}
	for wait > 0 {
		time.Sleep(w)
		wait -= w
		
		ok := p.lastAck.Add(p.packetTimeout).After(time.Now())
		if ok {
			return true
		}
	}
	
	return p.lastAck.Add(p.packetTimeout).After(time.Now())
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
	p.connection.queuePacketForSend(packet)
}
