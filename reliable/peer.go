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
	outgoingPackets chan *packet
	closed bool
	done chan struct{}
	
	lastDispatch time.Time
	dispatchLevel int
	rttChan chan time.Duration
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
	p.rttChan = make(chan time.Duration)
	
	go p.rttMonitor()
	
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
	pkt, ok := p.UnAckedPackets[ack]
	if ok {
		log.Infof("%v Removing unacked packet with seq %d", p, ack)
		delete(p.UnAckedPackets, ack)
		
		p.updateRtt(pkt)
	}
}

func (p *peer) updateRtt(pkt *packet) {
	now := time.Now()
	
	rtt := now.Sub(pkt.timestamp)
	if rtt < 0 {
		log.Warnf("Calculated RTT < 0, this isn't possible (%v)", rtt)
		return
	}
	
	select {
		case <- p.done:
		case p.rttChan <- rtt:
	}
}

func (p *peer) rttMonitor() {
	rttList := make([]time.Duration, 0)
		
	for {
		select {
			case <- p.done:
				return
			case rtt := <- p.rttChan:
				rttList = append(rttList, rtt)
				
				if len(rttList) < rtt_thresh_check_min_packets {
					continue
				}
				if len(rttList) > rtt_thresh_check_min_packets {
					rttList = append(rttList[:0], rttList[len(rttList) - rtt_thresh_check_min_packets:]...)
				}
				
				var total time.Duration
				for _, r := range rttList {
					total += r
				}
				
				ave := total / time.Duration(len(rttList))
				currentInterval := p.dispatchInterval()
				
				if ave > rtt_thresh_high {
					if currentInterval < dispatch_queue_quality_min {
						p.dispatchLevel++
						log.Infof("RTT average (%v) is too high, increasing sending delay to %v", ave, p.dispatchInterval())
					} else {
						log.Warnf("RTT average (%v) is too high but already at highest sending delay", ave)
					}
				} else if ave < rtt_thresh_low && currentInterval > dispatch_queue_quality_max {
					p.dispatchLevel--					
				} else {
					log.Infof("Average RTT within bounds for current dispatch (%v): %v", ave, currentInterval)
				}
				
		}
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
	d := time.Duration(dispatch_queue_quality_max + time.Duration(p.dispatchLevel) * dispatch_queue_quality_step)
	return d
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














