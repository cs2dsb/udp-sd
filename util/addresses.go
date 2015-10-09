package util

import (
	"net"
)

var interfacesMulticast []*net.Interface
var interfacesUnicast []*net.Interface

func GetMulticastInterfaces() []*net.Interface {
	if len(interfacesMulticast) == 0 {
		allInterfaces, err := net.Interfaces()
		if err != nil {
			panic(err)
		}
		
		interfacesMulticast = make([]*net.Interface, 0)
		
		for _, ifc:= range allInterfaces {
			if ifc.Flags & net.FlagUp == net.FlagUp && ifc.Flags & net.FlagMulticast == net.FlagMulticast {
				interfacesMulticast = append(interfacesMulticast, &ifc)
			}
		}
	}
	
	return interfacesMulticast
}

func GetUnicastInterfaces() []*net.Interface {
	if len(interfacesUnicast) == 0 {
		allInterfaces, err := net.Interfaces()
		if err != nil {
			panic(err)
		}
		
		interfacesUnicast = make([]*net.Interface, 0)
		
		for _, ifc:= range allInterfaces {
			if ifc.Flags & net.FlagUp == net.FlagUp  {
				interfacesUnicast = append(interfacesUnicast, &ifc)
			}
		}
	}
	
	return interfacesUnicast
}