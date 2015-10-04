package main

import (
	"udp-sd/sd"
	"fmt"
	"sync"
	"math/rand"
	"time"
	"udp-sd/reliable"
	"net"
)

func main() {
	r1, err := reliable.NewReliableConnection()
	if err != nil {
		fmt.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r1 == nil {
		fmt.Errorf("NewReliableConnection() returned nil connection")
	}
	
	r2, err := reliable.NewReliableConnection()
	if err != nil {
		fmt.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r2 == nil {
		fmt.Errorf("NewReliableConnection() returned nil connection")
	}
	
	p := r1.ConnectPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		fmt.Errorf("ConnectPeer() returned nil peer")
	}
	
	isAlive := p.IsAlive(time.Millisecond * 2000)
	if !isAlive {
		fmt.Errorf("peer.IsAlive(2000ms) returned false")
	}	
	r1.EnableKeepalive()
	
	select{}
	return
	services := []sd.Service{
		sd.Service { Name: "a", RequiredServices: []string{} },
		/*sd.Service { Name: "b", RequiredServices: []string{ "a" } },
		sd.Service { Name: "c", RequiredServices: []string{ "a" } },
		sd.Service { Name: "d", RequiredServices: []string{ "a", "c" } },
		sd.Service { Name: "e", RequiredServices: []string{ "b" } },
		sd.Service { Name: "f", RequiredServices: []string{ "b" } },
		sd.Service { Name: "g", RequiredServices: []string{ "b" } },
		sd.Service { Name: "h", RequiredServices: []string{ "b", "c" } },
		sd.Service { Name: "i", RequiredServices: []string{ "e", "f" } },
		sd.Service { Name: "j", RequiredServices: []string{ "c",  "d"  } },
		sd.Service { Name: "k", RequiredServices: []string{ "i", "g", "h", "j" } },		*/
	}
	
	var wg sync.WaitGroup
	wg.Add(len(services))
	
	for _, s := range services {
		go func(s sd.Service) {
			jitter := time.Microsecond * time.Duration(rand.Float64() * 10000.0)
			time.Sleep(jitter)
			err := s.BringOnline()
			if err != nil {
				panic(err)
			}
			err = s.WaitForOnline()
			if err != nil {
				panic(err)
			}
			fmt.Printf("Service '%s' came online\n", s.Name)
			wg.Done()
		}(s)
	}
	
	fmt.Println("Waiting for services to come online")
	wg.Wait()
	fmt.Println("All services online")
	
	select {}
}