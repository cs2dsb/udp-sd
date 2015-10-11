package util

import (
	"testing"
)

func TestGetUnicastInterfacesReturnsSomething(t *testing.T) {
	addresses := GetUnicastInterfaces()
	if len(addresses) == 0 {
		t.Fatalf("GetUnicastInterfaces returned nothing")
	}
}