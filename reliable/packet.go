package reliable

import (
	"fmt"
	"encoding/binary"
	"time"
	log "github.com/Sirupsen/logrus"
)

const (
	protocol_id = uint32(234545346)
	header_length = 4 + 1 + 4 * 5 //protocol_id, opcode, retries, retries used, ack, seq and ackbitfield
)

type encodedPacket struct {
	Peer *peer
	Payload *[]byte
}

type packet struct {
	Peer *peer
	OpCode opCode
	Payload []byte
	Retries uint32
	RetriesUsed uint32
	Seq uint32
	Ack uint32
	AckBitfield uint32
	timestamp time.Time
}

func (p *packet) String() string {
	pl := len(p.Payload)
	return fmt.Sprintf("%s from %v with %d byte payload (Ack: %d, Seq: %d, AckBitfield:%d, Retries: %d)", p.OpCode, p.Peer, pl, p.Ack, p.Seq, p.AckBitfield, p.Retries)
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

func uint32ToBytes(num uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, num)
	return buf
}

func uint32FromBytes(buf []byte) (uint32, int) {
	num := binary.BigEndian.Uint32(buf)
	return num, 4
}

func (op *packet) toBytes() *[]byte {
	fields := [][]byte {
		uint32ToBytes(protocol_id),
		*op.OpCode.toBytes(),
		uint32ToBytes(op.Ack),
		uint32ToBytes(op.Seq),
		uint32ToBytes(op.AckBitfield),
		uint32ToBytes(op.Retries),
		uint32ToBytes(op.RetriesUsed),
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
	
	protId := uint32ToBytes(protocol_id)	
	
	
	p := *ip.Payload
	if len(p) < header_length {
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
	
	pkt.Retries, nn = uint32FromBytes(p[n:])
	n += nn
	
	pkt.RetriesUsed, nn = uint32FromBytes(p[n:])
	n += nn
	
	buf := p[n:]
	pkt.Payload = buf
	
	return &pkt, nil	
}

func newPacketWithRetries(peer *peer, payload []byte, retries uint32, opCode opCode) *packet {
	return &packet{
		Peer: peer,
		Payload: payload,
		Retries: retries,
		OpCode: opCode,
	}
}

func newPacket(peer *peer, payload []byte, opCode opCode) *packet {
	return newPacketWithRetries(peer, payload, default_retries, opCode)
}