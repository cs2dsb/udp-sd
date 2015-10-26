package wire_format

import (
	"fmt"
	
	log "github.com/Sirupsen/logrus"
)

type ServiceAddress struct {	
	Id Identifier
}

func NewServiceAddress(name string) *ServiceAddress {
	sa := &ServiceAddress {
		Id: *NewIdentifier(name),
	}
	return sa
}

func (sa *ServiceAddress) String() string {
	return fmt.Sprintf("[ServiceAddress { Id: %v }]", sa.Id)
}

func (sa *ServiceAddress) GetUniqueToken() byte {
	return 1
}

func (sa *ServiceAddress) SerializableConstructor (chunk *Chunk) Serializable {	
	ret := &ServiceAddress{}

	if chunk == nil || len(chunk.buffer) == 0{
		return ret
	}
	
	err := DeserializeToken(ret, chunk)
	if err != nil {
		log.Errorf("Error deserializing token from ServiceAddress chunk: %v", err)
		return nil		
	}
	
	var buf []byte
	err = DeserializeBinary(chunk, &buf)
	if err != nil {
		log.Errorf("Error deserializing id from ServiceAddress chunk: %v", err)
		return nil	
	}
	
	ret.Id = *ret.Id.SerializableConstructor(&Chunk{ buffer: buf }).(*Identifier)
		
	if !chunk.AtEnd() {
		log.Errorf("More bytes left after deserializing ServiceAddress chunk: %v", chunk.GetHead())
	}
	
	return ret
}

func (sa *ServiceAddress) Serialize() []byte {
	parts := [][]byte {
		SerializeToken(sa),
		SerializeSerializable(&sa.Id),
	}
	return MergeParts(parts)
}

func (sa *ServiceAddress) Equals(otherObject Serializable) bool {
	osa, ok := otherObject.(*ServiceAddress)
	if !ok {
		return false
	}
	
	return sa.Id.Equals(&osa.Id)
}