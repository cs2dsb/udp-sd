package announcer

import (
	"net"
	"testing"	
	"github.com/satori/go.uuid"
)

func TestPeerlistGetRandomPeers(t *testing.T) {
	pL := NewPeerList()
	p1 := &announcePeer {
		Id: uuid.Nil,
		ServicesOffered: make([]string, 0),
		Addresses: make([]*net.UDPAddr, 0),
	}
	p2 := &announcePeer {
		Id: uuid.Nil,
		ServicesOffered: make([]string, 0),
		Addresses: make([]*net.UDPAddr, 0),
	}
	p3 := &announcePeer {
		Id: uuid.Nil,
		ServicesOffered: make([]string, 0),
		Addresses: make([]*net.UDPAddr, 0),
	}
	
	var a [16]byte
	a[0] = 1
	p1.Id = a
	
	var b [16]byte
	b[0] = 2
	p2.Id = b
	
	var c [16]byte
	c[0] = 3
	p3.Id = c
	
	pL.addOrUpdateAnnouncePeer(p1)
	pL.addOrUpdateAnnouncePeer(p2)
	pL.addOrUpdateAnnouncePeer(p3)
	
	if len(pL.Peers) != 3 {
		t.Errorf("Expected peerlist to contain 3 peers, got %d", len(pL.Peers))
		return
	}
	
	randPeers := pL.getRandomPeers(1, announcePeerList { p1 }, peerHasId)
	if len(randPeers) != 1 {
		t.Errorf("Expected to get 1 random peer, got %d", len(randPeers))
		return
	}
	
	if randPeers[0].Equals(p1) {
		t.Errorf("getRandomPeers returned a peer from the exclusion list")
		return
	}
	
	randPeers = pL.getRandomPeers(1, announcePeerList { p1, p2 }, peerHasId)
	if len(randPeers) != 1 {
		t.Errorf("Expected to get 1 random peer, got %d", len(randPeers))
		return
	}
	if randPeers[0].Equals(p1) || randPeers[0].Equals(p2) {
		t.Errorf("getRandomPeers returned a peer from the exclusion list")
		return
	}
	
	randPeers = pL.getRandomPeers(2, announcePeerList { p1 }, peerHasId)
	if len(randPeers) != 2 {
		t.Errorf("Expected to get 2 random peer, got %d", len(randPeers))
		return
	}	
	if randPeers[0].Equals(p1) || randPeers[1].Equals(p1) {
		t.Errorf("getRandomPeers returned a peer from the exclusion list")
		return
	}
	
	randPeers = pL.getRandomPeers(3, announcePeerList { p1 }, peerHasId)
	if len(randPeers) != 2 {
		t.Errorf("Expected to get 2 random peers, got %d", len(randPeers))
		return
	}	
	if randPeers[0].Equals(p1) || randPeers[1].Equals(p1) {
		t.Errorf("getRandomPeers returned a peer from the exclusion list")
		return
	}
	
	randPeers = pL.getRandomPeers(3, nil, peerHasId)
	if len(randPeers) != 3 {
		t.Errorf("Expected to get 3 random peer, got %d", len(randPeers))
		return
	}	
	
	
	
}














