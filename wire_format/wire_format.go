package wire_format

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
}