package main

import (
	"runtime"
	"fmt"
	"time"
	"github.com/cs2dsb/udp-sd/reliable"
	"net"
)

func main() {
	fmt.Printf("GoMaxProcs: %d\n", runtime.GOMAXPROCS(0))
	/*addr := &net.UDPAddr{
        Port: 59229,
        IP: net.ParseIP("127.0.0.1"),
    }
    conn, err := net.ListenUDP("udp", addr)
    defer conn.Close()
    if err != nil {
        panic(err)
    }
	
	addr2 := &net.UDPAddr{
        Port: 59230,
        IP: net.ParseIP("127.0.0.1"),
    }
	//conn2, err := net.DialUDP("udp", addr2, addr)
	conn2, err := net.ListenUDP("udp", addr2)
    defer conn2.Close()
    if err != nil {
        panic(err)
    }
	
	go func() {
		incomingBuf := make([]byte, 1500)
		for {
			n, _, err := conn.ReadFromUDP(incomingBuf)			
			if err != nil {
				panic(err)
			}
			//fmt.Printf("Got data")
			conn.WriteToUDP(incomingBuf[:n], addr)
		}
	}()
	
	data := []byte("Abjkfdjskljdklfjdskljdskljfklds")
	incomingBuf := make([]byte, 1500)
	var rttTot time.Duration
	rttCount := 0
	
	go func() {
		for {
			<- time.After(time.Second * 2)
			rttAve := time.Duration(-1)
			if rttCount > 0 {
				rttAve = rttTot / time.Duration(rttCount)
			}
			fmt.Printf("Rtt Average: %v\n", rttAve)
		}
	}()
	
	for {
		start := time.Now()
		_, err := conn2.WriteTo(data, addr)
		//fmt.Printf("Wrote %d bytes\n", i)
		if err != nil {
			panic(err)
		}
		
		_, _, err = conn.ReadFromUDP(incomingBuf)			
		if err != nil {
			panic(err)
		}
		end := time.Now()
		rttTot += end.Sub(start)
		rttCount ++
	}
	
	return*/
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
	
	p := r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		fmt.Errorf("ConnectPeer() returned nil peer")
	}
	
	isAlive := p.IsAlive(time.Millisecond * 2000)
	if !isAlive {
		fmt.Errorf("peer.IsAlive(2000ms) returned false")
	}	
	
	select{}	
}