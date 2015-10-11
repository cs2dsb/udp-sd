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
	packetTimeout time.Duration
	connectionTimeout time.Duration
	connection Connection
	outgoingPackets chan *packet
	done chan struct{}
	sweepInterval time.Duration
	
	packetOperations chan *packetOperation
	stateQueries chan *stateQuery
	
	lastDispatch time.Time
	dispatchLevel int
	rttChan chan time.Duration
}

func NewPeer(address *net.UDPAddr, c Connection) *peer {
	p := &peer{}
	p.connection = c
	p.sweepInterval = c.GetSweepInterval()
	p.Address = address
	p.Seq = 1
	p.packetOperations = make(chan *packetOperation)
	p.stateQueries = make(chan *stateQuery)
	p.connectionTimeout = c.getConnectionTimeout()
	p.packetTimeout = c.getPacketTimeout()
	//Short queue for dispatch thread
	p.outgoingPackets = make(chan *packet, 10)
	p.done = make(chan struct{})
	p.lastAck = time.Now().Add(-keepalive_timeout)	
	p.rttChan = make(chan time.Duration)
	p.dispatchLevel = dispatch_quality_default_level
	
	go p.packetLoop()
	go p.rttMonitor()
	
	return p
}

type Peer interface {
	SendData(payload []byte)
	SendPacket(pkt *packet)
	ReceivePacket(pkt *packet)
	IsAlive(wait time.Duration) bool
	Equal(p2 Peer) bool
	GetAddress() *net.UDPAddr
}

type stateQueryType int

const (
	queryListUnacked stateQueryType = 1 << iota	
	queryGetUnackedBySeq
	queryGetIsPacketUnacked
)

type stateQuery struct {
	QueryType stateQueryType
	QueryExtraInfo interface{}
	Resp chan interface{}
}

func (sq *stateQuery) reply(r interface{}, p *peer) {
	select {
		case <- p.done:
			close(sq.Resp)
		case sq.Resp <- r:
	}
}

func (sq *stateQuery) send(p *peer) {
	select {
		case <- p.done:
		case p.stateQueries <- sq:
	}
}

type packetOperationType int

const (
	packetSend	packetOperationType = 1 << iota
	packetReceive
	packetAck
	packetResend
)

type packetOperation struct {
	PacketOperation packetOperationType
	Packet *packet
}

func (p *peer) packetLoop() {
	unAckedPackets := make(map[uint32]*packet)
	sweepTicker := time.NewTicker(p.sweepInterval)
	outgoing := make(chan *packet)
	
	go func() {
		outgoingQueue := make([]*packet, 0)
		
		for {
			if len(outgoingQueue) > 0 {
				pkt := outgoingQueue[0]
				select {
					case p.outgoingPackets <- pkt:
						//Underlying structure will get sorted next time something gets added
						outgoingQueue = outgoingQueue[1:]
					default: //queue is full
				}
			
				//Fetch but don't block
				select {
					case <- p.done:
						return
					case pkt := <- outgoing:
						outgoingQueue = append(outgoingQueue, pkt)
					default:
				}		
			} else {
				//blocking fetch
				select {
					case <- p.done:
						return
					case pkt := <- outgoing:
						outgoingQueue = append(outgoingQueue, pkt)
				}
			}
		}
	}()
	
	updateAndSend := func(pkt *packet) {
		//Update it's sequence number and move the peer one forward
		pkt.Seq = p.Seq
		p.Seq++
		
		pkt.Ack = p.RemoteSeq
		pkt.AckBitfield = p.AckBitfield
		
		//Update timestamp
		pkt.timestamp = time.Now()

		//Add it to outstanding packets list if we want an ack from the receiver
		if pkt.OpCode & opAck != opAck {
			unAckedPackets[pkt.Seq] = pkt
		}
		
		//Send it up to the above goroutine for real sending
		outgoing <- pkt			
	}
	
	resendPacket := func(pkt *packet) {		
		pRetry := pkt.Copy()
		pRetry.RetriesUsed++
		
		updateAndSend(pRetry)
		log.Infof("-RETRY- %v retrying packet %d as %d", p.connection, pkt.Seq, pRetry.Seq)										
	}
	
	resendExpiredPackets := func() {
		now := time.Now()
		for _, pkt := range unAckedPackets {
			if pkt.timestamp.Add(p.packetTimeout).After(now) {
				continue
			}
			
			delete(unAckedPackets, pkt.Seq)
			if pkt.Retries > pkt.RetriesUsed {
				resendPacket(pkt)
			} else {
				log.Infof("-DROP- %v dropping packet %d that ran out of retries (%d/%d)", p.connection, pkt.Seq, pkt.RetriesUsed, pkt.Retries)
			}
		}
	}
	
	processPacketOperation := func(po *packetOperation) {
		if po == nil {
			log.Warnf("Got a nil packet operation on packet operations channel")
			return
		}
		
		if po.Packet == nil {
			log.Warnf("Got a packet operation with a nil packet on packet operations channel")
			return
		}
		
		pkt := po.Packet
		
		switch po.PacketOperation {
			case packetSend:
				updateAndSend(pkt)				
				
			case packetReceive:
				//Record that we got a packet just now
				p.lastAck = time.Now()
		
				//Ack any packets of ours that this packet references
				acks := pkt.ackList()
				for _, a := range acks {
					oldPkt, ok := unAckedPackets[a]
					if ok {
						delete(unAckedPackets, a)
						p.updateRtt(oldPkt)
						log.Infof("-ACKED- %v removing acked packet %d (%v)", p.connection, a, oldPkt.OpCode)
					}
				}				
				//Update our seq for the remote end and update ack bitfield
				p.updateRemoteSeq(pkt.Seq)
				
				//Ack if it's reliable
				if pkt.OpCode & opReliable == opReliable {		
					log.Infof("-ACKING- %v: Sending Ack for %d (%v) to %v", p.connection, pkt.Seq, pkt.OpCode, p)
					pAck := pkt.CreateAck()
					updateAndSend(pAck)
				}
				
			case packetResend:
				resendPacket(pkt)
		}
	}
	
	processStateQuery := func(sq *stateQuery) {
		switch sq.QueryType {
			case queryListUnacked:
				list := make([]*packet, 0)
				for _, pkt := range unAckedPackets {
					list = append(list, pkt)
				}
				sq.reply(list, p)
				
			case queryGetUnackedBySeq:
				seq := sq.QueryExtraInfo.(uint32)
				pkt, _ := unAckedPackets[seq]
				sq.reply(pkt, p)
				
			case queryGetIsPacketUnacked:
				pkt := sq.QueryExtraInfo.(*packet)				
				found := false
				for _, pkt2 := range unAckedPackets {
					if pkt == pkt2 {
						found = true
						break
					}
				}
				sq.reply(found, p)
		}
	}		
	
	for {
		select {
			case <- p.done:
				return
			case po := <- p.packetOperations:
				processPacketOperation(po)
			case sq := <- p.stateQueries:
				processStateQuery(sq)
			case <-sweepTicker.C:
				resendExpiredPackets()
		}
		
	}	
}

func (p *peer) GetPacketIsPacketUnacked(pkt *packet) bool {
	if pkt == nil {
		return false
	}
	
	query := &stateQuery {
		QueryType: queryGetIsPacketUnacked,
		QueryExtraInfo: pkt,
		Resp: make(chan interface{}),
	}
	
	query.send(p)
	
	select {
		case <- p.done:
			return false
		case r := <- query.Resp:
			return r.(bool)
	}
}

func (p *peer) GetUnackedPacketList() []*packet {	
	query := &stateQuery {
		QueryType: queryListUnacked,
		Resp: make(chan interface{}),
	}
	
	query.send(p)
	
	select {
		case <- p.done:
			return nil
		case r := <- query.Resp:
			return r.([]*packet)
	}
}

func (p *peer) GetUnackedPacketBySeq(seq uint32) *packet {
	query := &stateQuery {
		QueryType: queryGetUnackedBySeq,
		QueryExtraInfo: seq,
		Resp: make(chan interface{}),
	}
	
	query.send(p)
	
	select {
		case <- p.done:
			return nil
		case r := <- query.Resp:
			return r.(*packet)
	}
}

func (p *peer) IsClosed() bool {
	select {
		case <-p.done:
			return true
		default:
			return false
	}
}

func (p *peer) ReceivePacket(pkt *packet) {
	if pkt == nil {
		log.Warnf("peer.ReceivePacket called with nil packet")
		return
	}
	po := &packetOperation{
		PacketOperation: packetReceive,
		Packet: pkt,
	}
	
	select {
		case p.packetOperations <- po:
		case <- p.done:
			log.Warnf("peer.ReceivePacket called on closed peer")
	}
}

func (p *peer) SendPacket(pkt *packet) {
	if pkt == nil {
		log.Warnf("peer.SendPacket called with nil packet")
		return
	}
	
	po := &packetOperation{
		PacketOperation: packetSend,
		Packet: pkt,
	}
	
	select {
		case p.packetOperations <- po:
		case <- p.done:
			log.Warnf("peer.SendPacket called on closed peer")
	}
}

func (p *peer) GetAddress() *net.UDPAddr {
	return p.Address
}

func (p *peer) Close() {
	select {
		case <- p.done:
		default:
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

/*func (p *peer) waitForAck(seq uint32, wait time.Duration) bool {
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
}*/


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
					if currentInterval < dispatch_quality_min {
						p.dispatchLevel++
						rttList = make([]time.Duration, 0)			
						log.Infof("RTT average (%v) is too high, increasing sending delay to %v", ave, p.dispatchInterval())
					} else {
						log.Warnf("RTT average (%v) is too high but already at highest sending delay", ave)
					}
				} else if ave < rtt_thresh_low && currentInterval > dispatch_quality_max {
					p.dispatchLevel--		
					rttList = make([]time.Duration, 0)			
					log.Infof("RTT average (%v) is low, decreasing sending delay to %v", ave, p.dispatchInterval())
				} else {
					log.Infof("RTT average (%v) within bounds for current dispatch: %v", ave, currentInterval)
				}
				
		}
	}
}

func (p *peer) SendData(payload []byte) {
	packet := newPacket(p, payload, opData)
	p.SendPacket(packet)
}

func (p *peer) dispatchInterval() time.Duration {
	/*d := time.Duration(dispatch_quality_max + time.Duration(p.dispatchLevel) * dispatch_quality_step)
	return d*/
	return dispatch_levels[p.dispatchLevel]
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
	log.Infof("-SENT- %v sent packet %d (%v) to %v", p.connection, pkt.Seq, pkt.OpCode, p)
}

func (p *peer) keepalive(now time.Time) {
	if p.lastDispatch.Add(keepalive_timeout).After(now) {
		return
	}
	p.connection.sendKeepaliveToPeer(p)
}

func (p *peer) Equal(p2 Peer) bool {
	if p == nil || p2 == nil {
		return false
	}
	p2Addr := p2.GetAddress()
	if p2Addr == nil || p.Address == nil {
		return false
	}
	return p.Address.IP.Equal(p2Addr.IP) && p.Address.Port == p2Addr.Port
}












