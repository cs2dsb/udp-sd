package wire_format

import (
	"fmt"
	"time"
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
	if chunk == nil {
		return nil
	}
	
	ret := &Identifier{}
	
	return ret
}

func (i *Identifier) Serialize() []byte {
	return []byte{ i.GetUniqueToken() }	
}