package reliable

import (
	"fmt"
	"encoding/binary"
)

type opCode uint8

const (
	opAck opCode = 1 << iota
	opData
	opPing
	opReliable
)

func (o opCode) String() string {
	r := ""
	
	if o & opAck == opAck {
		r += "opAck"
	}
	
	if o & opData == opData {
		if len(r) > 0 {	r += " | " }
		r += "opData"
	}
	
	if o & opPing == opPing {
		if len(r) > 0 {	r += " | " }
		r += "opPing"
	}
	
	if o & opReliable == opReliable {
		if len(r) > 0 {	r += " | " }
		r += "opReliable"
	}
	
	return r
}


func (o opCode) toBytes() *[]byte {
	buf := make([]byte, binary.Size(o))
	binary.PutUvarint(buf, uint64(o))
	
	return &buf
}

func getOpCodeFromBytes(buf []byte) (opCode, int, error) {
	minLength := binary.Size(opAck)
	if len(buf) < minLength {
		return 0, 0, fmt.Errorf("Not enough bytes to be an opCode")
	}
	
	v, n := binary.Uvarint(buf)
	return opCode(v), n, nil
}
