package reliable

import (
	"fmt"
	"time"
	"encoding/binary"
	log "github.com/Sirupsen/logrus"
)

type encodedPacket struct {
	Peer *peer
	Payload *[]byte
}

type packet struct {
	Peer *peer
	OpCode opCode
	Payload []byte
	retries int
	Seq uint32
	Ack uint32
	AckBitfield uint32
	timestamp time.Time
}

func (p *packet) String() string {
	pl := len(p.Payload)
	return fmt.Sprintf("%s from %v with %d byte payload (Ack: %d, Seq: %d, AckBitfield:%d, Retries: %d)", p.OpCode, p.Peer, pl, p.Ack, p.Seq, p.AckBitfield, p.retries)
}

func (op *packet) ackList() []uint32 {
	list := []uint32{op.Ack}
	
	for i := uint32(0); i < 32; i++ {
		mask := uint32(1 << i)
		if op.AckBitfield & mask == mask {
			list = append(list, op.Ack - (i + 1))
		}
	}
	
	return list
}

func (op *packet) toBytes() *[]byte {
	fields := [][]byte {
		*getProtocolIdBytes(),
		*op.OpCode.toBytes(),
		uint32ToBytes(op.Ack),
		uint32ToBytes(op.Seq),
		uint32ToBytes(op.AckBitfield),
		op.Payload,
	}
	
	rl := 0
	for _, f := range fields {
		rl += len(f)
	}
	
	r := make([]byte, rl)
		
	n := 0
	for _, f := range fields {
		n += copy(r[n:], f)
	}	
	
	return &r
}

func (ip encodedPacket) toOutgoingPacket() (*packet, error) {	
	if ip.Payload == nil {
		err := fmt.Errorf("Incoming packet payload was nil, can't reassemble")
		log.Error(err)
		return nil, err
	}

	pkt := packet{
		Peer: ip.Peer,
	}
	
	protId := *getProtocolIdBytes()	
	minLength := len(protId) + 
				binary.Size(pkt.OpCode) +
				binary.Size(pkt.Ack) +
				binary.Size(pkt.Seq) + 
				binary.Size(pkt.AckBitfield)
	
	p := *ip.Payload
	if len(p) < minLength {
		err := fmt.Errorf("Incoming packet payload was too small to be valid, can't reassemble")
		log.Error(err)
		return nil, err
	}
	
	for i, v := range protId {
		if v != p[i] {
			err := fmt.Errorf("Incoming packet had invalid protocol_id")
			log.Error(err)
			return nil, err
		}
	}
		
	n := len(protId)
	var nn int
	var err error
	
	pkt.OpCode, nn, err = getOpCodeFromBytes(p[n:])
	if err != nil {
		err := fmt.Errorf("Incoming packet had invalid opCode: %v", err)
		log.Error(err)
		return nil, err
	}
	n += nn
	
	pkt.Ack, nn = uint32FromBytes(p[n:])
	n += nn
	
	pkt.Seq, nn = uint32FromBytes(p[n:])
	n += nn
	
	pkt.AckBitfield, nn = uint32FromBytes(p[n:])
	n += nn
	
	buf := p[n:]
	pkt.Payload = buf
	
	return &pkt, nil	
}

func newPacketWithRetries(peer *peer, payload []byte, retries int, opCode opCode) *packet {
	return &packet{
		Peer: peer,
		Payload: payload,
		retries: retries,
		OpCode: opCode,
	}
}

func newPacket(peer *peer, payload []byte, opCode opCode) *packet {
	return newPacketWithRetries(peer, payload, default_retries, opCode)
}