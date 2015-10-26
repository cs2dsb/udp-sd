package wire_format

import (
	"fmt"
	"time"
	
	log "github.com/Sirupsen/logrus"
)

type Identifier struct {
	Time time.Time
	Name string
}

func NewIdentifier(name string) *Identifier {
	i := &Identifier{
		Time: time.Now(),
		Name: name,
	}
	return i
}

func (i Identifier) String() string {
	return fmt.Sprintf("[Identifier { Name: \"%s\", Time: \"%v\" }]", i.Name, i.Time)
}

func (i *Identifier) GetUniqueToken() byte {
	return 0
}

func (i *Identifier) SerializableConstructor (chunk *Chunk) Serializable {
	ret := &Identifier{}
	
	if chunk == nil || len(chunk.buffer) == 0{
		return ret
	}
	
	err := DeserializeToken(ret, chunk)
	if err != nil {
		log.Errorf("Error deserializing token from Identifier chunk: %v", err)
		return nil		
	}
	
	err = DeserializeTime(chunk, &ret.Time)
	if err != nil {
		log.Errorf("Error deserializing time from Identifier chunk: %v", err)
		return nil	
	}
	
	err = DeserializeString(chunk, &ret.Name)
	if err != nil {
		log.Errorf("Error deserializing name from Identifier chunk: %v", err)
		return nil	
	}
	
	if !chunk.AtEnd() {
		log.Errorf("More bytes left after deserializing Identifier chunk")
	}
	
	return ret
}

func (i *Identifier) Serialize() []byte {
	parts := [][]byte {
		SerializeToken(i),
		SerializeTime(i.Time),
		SerializeString(i.Name),
	}
	return MergeParts(parts)
}

func (i *Identifier) Equals(otherObject Serializable) bool {
	oi, ok := otherObject.(*Identifier)
	if !ok {
		return false
	}
	
	return i.Time.Equal(oi.Time) && i.Name == oi.Name
}