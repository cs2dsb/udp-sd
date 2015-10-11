//+build multicast

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