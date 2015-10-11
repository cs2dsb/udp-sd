package announcer

import (
	"time"
	"testing"
	"fmt"
	"github.com/cs2dsb/udp-sd/util"
)

func getAnnouncer() (*announcer, *announcePeer, error) {
	a, err := NewAnnouncer([]string{"nothing much"}, false)
	if err != nil {
		return nil, nil, fmt.Errorf("Error returned from NewAnnouncer: %v", err)
	}
	if a == nil {
		return nil, nil, fmt.Errorf("nil announcer returned from NewAnnouncer")
	}
	
	p := a.peers.getPeer(a.id) 	
	if p == nil {
		return nil, nil, fmt.Errorf("Failed to find announcers own peer with getPeer(id)")
	}
		
	return a, p, nil
}

func TestAnnouncerGetsAddresses(t *testing.T) {
	_, p, err := getAnnouncer()
	if err != nil {
		t.Error(err)
		return
	}
	
	if len(p.Addresses) == 0 {
		t.Errorf("Announcers own peer has no interface addresses")
	}
}

func TestAnnouncerGetsOnlySupportedAddresses(t *testing.T) {
	_, p, err := getAnnouncer()
		if err != nil {
			t.Error(err)
			return
		}		

	for _, a := range p.Addresses {
		if a.IP.To4() == nil {
			t.Errorf("Announce peer has a non-ipv4 address (%v) which is currently not supported", a)
		}
	}
}

func TestAnnouncersCanCommunicate(t *testing.T) {
	a1, _, err := getAnnouncer()
	if err != nil {
		t.Fatal(err)		
	}
	
	a2, p2, err := getAnnouncer()
	if err != nil {
		t.Fatal(err)			
	}
	
	if len(a1.peers.Peers) != 1 {
		t.Fatalf("Announcer had %d peers, expected 1", len(a1.peers.Peers))		
	}
	
	if len(a2.peers.Peers) != 1 {
		t.Fatalf("Announcer had %d peers, expected 1", len(a2.peers.Peers))
	}
	
	a1.addPeer(p2.Addresses[0])
	err = a1.runAnnounceRound()
	if err != nil {
		t.Fatalf("Error from runAnnounceRound: %v", err)
	}
	
	if len(a1.peers.Peers) != 2 {
		t.Fatalf("Announcer had %d peers, expected 2", len(a1.peers.Peers))
	}
	
	ok := util.WaitUntilTrue(func() bool {
		return len(a2.peers.Peers) == 2
	}, time.Second * 5)
	
	if !ok {
		t.Fatalf("Announcer didn't receive announcement within 5 seconds")
	}
	
	peers := a2.unicastConnection.GetPeerList()
	if len(peers) != 1 {
		t.Fatalf("Announcer got announcement but underlying connection reports %d peers instead of 1", len(peers))
	}
		
	
	a2.runAnnounceRound()
	
	ok = util.WaitUntilTrue(func() bool {
		return len(a1.peers.Peers) == 2
	}, time.Second * 5)
	
	if !ok {
		t.Fatalf("Announcer didn't receive announcement within 5 seconds")
	}
}

func TestPeersFindOutAboutEachOtherViaThirdParty(t *testing.T) {
	a1, _, err := getAnnouncer()
	if err != nil {
		t.Fatal(err)		
	}
	
	a2, p2, err := getAnnouncer()
	if err != nil {
		t.Fatal(err)			
	}
	
	a3, _, err := getAnnouncer()
	if err != nil {
		t.Fatal(err)			
	}
	
	if len(a1.peers.Peers) != 1 {
		t.Fatalf("Announcer had %d peers, expected 1", len(a1.peers.Peers))		
	}
	
	if len(a2.peers.Peers) != 1 {
		t.Fatalf("Announcer had %d peers, expected 1", len(a2.peers.Peers))
	}
	
	if len(a3.peers.Peers) != 1 {
		t.Fatalf("Announcer had %d peers, expected 1", len(a3.peers.Peers))
	}
	
	a1.addPeer(p2.Addresses[0])
	err = a1.runAnnounceRound()
	if err != nil {
		t.Fatalf("Error from runAnnounceRound: %v", err)
	}
	
	if len(a1.peers.Peers) != 2 {
		t.Fatalf("Announcer had %d peers, expected 2", len(a1.peers.Peers))
	}
	
	a3.addPeer(p2.Addresses[0])
	err = a3.runAnnounceRound()
	if err != nil {
		t.Fatalf("Error from runAnnounceRound: %v", err)
	}
	
	if len(a3.peers.Peers) != 2 {
		t.Fatalf("Announcer had %d peers, expected 2", len(a3.peers.Peers))
	}
	
	ok := util.WaitUntilTrue(func() bool {
		return len(a2.peers.Peers) == 3
	}, time.Second * 5)
	
	if !ok {
		t.Fatalf("Announcer didn't receive announcements within 5 seconds")
	}
		
	ok = util.WaitUntilTrue(func() bool {
		return len(a1.peers.Peers) == 3 && len(a3.peers.Peers) == 3
	}, time.Second * 5)
	
	if !ok {
		t.Fatalf("Announcers didn't recieve announcements about peers via 3rd party within 5 seconds")
	}
}