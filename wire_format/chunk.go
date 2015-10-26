package wire_format

import (
	"fmt"
)

type Chunk struct {
	buffer []byte
	position int
}

func NewChunk(obj Serializable) *Chunk {
	c := &Chunk{}
	return c
}

func (c *Chunk) ResetPosition() {
	c.position = 0
}

func (c *Chunk) ReadBytes(n int) ([]byte, error) {
	if (c.position + n) > len(c.buffer) {
		return nil, fmt.Errorf("Not enough bytes in chunk, wanted %d, len is %d", c.position + n, len(c.buffer))
	}
	
	ret := c.buffer[c.position:c.position+n]
	
	c.position += n
	
	return ret, nil
}

func (c *Chunk) GetHead() []byte {
	if len(c.buffer) == 0 {
		return c.buffer
	}
	return c.buffer[c.position:]
}

func (c *Chunk) AtEnd() bool {
	return c.position == len(c.buffer)
}