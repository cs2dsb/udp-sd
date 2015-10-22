package wire_format

import (
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

func (i *Identifier) GetUniqueToken() byte {
	return 0
}