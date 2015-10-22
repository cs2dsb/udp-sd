package wire_format

type ServiceAddress struct {	
	Id *Identifier
}

func NewServiceAddress(name string) *ServiceAddress {
	sa := &ServiceAddress {
		Id: NewIdentifier(name),
	}
	return sa
}

func (sa *ServiceAddress) GetUniqueToken() byte {
	return 1
}