package wire_format

import (
	
)

type Chunk struct {
	buffer []byte
}

func NewChunk(obj Serializable) *Chunk {
	c := &Chunk{}
	return c
}