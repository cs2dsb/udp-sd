package util

import (
	"testing"
)

func TestGetMulticastInterfacesReturnsSomething(t *testing.T) {
	addresses := GetMulticastInterfaces()
	if len(addresses) == 0 {
		t.Fatalf("GetMulticastInterfaces returned nothing")
	}
}

func TestGetUnicastInterfacesReturnsSomething(t *testing.T) {
	addresses := GetUnicastInterfaces()
	if len(addresses) == 0 {
		t.Fatalf("GetUnicastInterfaces returned nothing")
	}
}