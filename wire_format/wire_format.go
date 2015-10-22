package wire_format

type Serializable interface {
	//Each implementing interface must return a unique byte (could be changed to > byte later but 255 types is more than enough for now).
	//Uniqueness enforced by test
	GetUniqueToken() byte
}