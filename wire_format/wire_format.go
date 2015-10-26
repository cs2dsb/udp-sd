package wire_format

import (
	"time"
	"encoding/binary"
	"fmt"
	
	log "github.com/Sirupsen/logrus"
)

var (
	serializableConstructors map[byte]Serializable
)

func init() {
	serializableConstructors = make(map[byte]Serializable)	
	
	serializableObjs := []Serializable{
		&Identifier{},
		&ServiceAddress{},
	}
	
	for _, o := range serializableObjs {
		serializableConstructors[o.GetUniqueToken()] = o
	}
}

type SerializableConstructor func(chunk *Chunk) Serializable 

type Serializable interface {
	//Each implementing interface must return a unique byte (could be changed to > byte later but 255 types is more than enough for now).
	//Uniqueness enforced by test
	GetUniqueToken() byte
	SerializableConstructor(chunk *Chunk) Serializable
	Serialize() []byte
	Equals(otherObject Serializable) bool
}

func SerializeUint(val uint64) []byte {
	buf := make([]byte, binary.Size(val))
	len := binary.PutUvarint(buf, val)	
	return buf[:len]
}

func WrapBufInLen(buf []byte) []byte {
	lenBuff := SerializeUint(uint64(len(buf)))
	ret := append(lenBuff, buf...)
	//log.Infof("Wrapping len %d, %v: %v", len(buf), buf, ret)
	return ret
}

func UnwrapField(chunk *Chunk) ([]byte, error) {
	fieldLen, read := binary.Uvarint(chunk.GetHead())
	chunk.ReadBytes(read)
	
	ret, err := chunk.ReadBytes(int(fieldLen))
	if err != nil {
		//log.Infof("Unwarap failed field len %d: %v", fieldLen, err)
		return nil, err
	}	
	
	//log.Infof("Unwarapped field len %d: %v", fieldLen, ret)
	
	return ret, nil
}

func SerializeString(value string) []byte {
	buf := []byte(value)	
	return WrapBufInLen(buf)
}

func DeserializeString(chunk *Chunk, out *string) error {
	buf, err := UnwrapField(chunk)
	
	if err != nil {
		return err
	}
	
	*out = string(buf)
	
	return nil
}

func DeserializeBinary(chunk *Chunk, out *[]byte) error {
	buf, err := UnwrapField(chunk)
	
	if err != nil {
		return err
	}
	
	*out = buf
	
	return nil
}

func SerializeTime(value time.Time) []byte {
	buf, err := value.MarshalBinary()
	if err != nil {
		log.Errorf("Failed to serialize time value %v: %v", value, err)
		return make([]byte, 0)
	}
	return WrapBufInLen(buf)
}

func SerializeSerializable(value Serializable) []byte {
	return WrapBufInLen(value.Serialize())
}

func DeserializeTime(chunk *Chunk, out *time.Time) error {
	timeField, err := UnwrapField(chunk)
	if err != nil {
		return err
	}
	
	err = out.UnmarshalBinary(timeField)
	return err
}

func SerializeToken(value Serializable) []byte {
	return []byte { value.GetUniqueToken() }
}

func DeserializeToken(obj Serializable, chunk *Chunk) error {
	chunk.ResetPosition()
	b, err := chunk.ReadBytes(1)
	if err != nil {
		err = fmt.Errorf("Error reading token byte from chunk: %v", err)
		return err
	}
	
	if b[0] != obj.GetUniqueToken() {
		return fmt.Errorf("Token in chunk doesn't match serializable type")
	}
	
	return nil
}

func MergeParts(parts [][]byte) []byte {
	if len(parts) == 0 {
		return make([]byte, 0)
	}
	ret := parts[0]
	for i := 1; i < len(parts); i++ {
		ret = append(ret, parts[i]...)
	}
	//fmt.Printf("Returning merged parts len: %d\n", len(ret))
	return ret
}