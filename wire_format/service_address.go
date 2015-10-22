package wire_format

import (
	"fmt"
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
	if chunk == nil {
		return nil
	}
	
	ret := &ServiceAddress{}
	
	return ret
}

func (sa *ServiceAddress) Serialize() []byte {
	return []byte{sa.GetUniqueToken()}
}