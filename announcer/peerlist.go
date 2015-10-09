package announcer

import (
	"fmt"
	"sync"
	"math/rand"
	"time"
	"net"
	"encoding/json"	
	"github.com/satori/go.uuid"
	log "github.com/Sirupsen/logrus"
)

var (
	//time so it's different per execution and rand multiple versions don't end up with the same date (unlikely but just in case)
	peerRand = rand.New(rand.NewSource(time.Now().Unix() + rand.Int63()))
	
	peerHasId = func(p *announcePeer) bool {
		return !uuid.Equal(uuid.Nil, p.Id)
	}
	
	peerHasAddress = func(p *announcePeer) bool {
		return len(p.Addresses) > 0
	}
)

type announcePeer struct {
	Id uuid.UUID
	Addresses []*net.UDPAddr
	ServicesOffered []string
}

func (a *announcePeer) String() string {
	return a.Id.String()
}

func NewAnnouncePeer(addresses ...*net.UDPAddr) *announcePeer {
	return &announcePeer {
		Id: uuid.Nil,
		Addresses: addresses,
		ServicesOffered: make([]string,0),
	}
}

func (a *announcePeer) Equals(b *announcePeer) bool {
	if uuid.Equal(uuid.Nil, a.Id) || uuid.Equal(uuid.Nil, b.Id) {
		for _, aa := range a.Addresses {
			for _, ba := range b.Addresses {
				if aa.IP.Equal(ba.IP) && aa.Port == ba.Port {
					return true
				}
			}
		}
		return false
	}
	return uuid.Equal(a.Id, b.Id)
}

type announcePeerList []*announcePeer

func (apl announcePeerList) Contains(peer *announcePeer) bool {
	if len(apl) == 0 { 
		return false
	}
	for _, p := range apl {
		if p.Equals(peer) {
			return true
		}
	}
	return false
}

type peerList struct {
	Peers announcePeerList
	peerMut *sync.Mutex
}

func NewPeerList() *peerList {
	pl := &peerList {
		Peers: make(announcePeerList, 0),
		peerMut: &sync.Mutex{},
	}
	return pl
}

func (pl *peerList) getPeerList() announcePeerList {
	pl.peerMut.Lock() 
	defer pl.peerMut.Unlock()
	curPeers := make(announcePeerList, len(pl.Peers))
	copy(curPeers, pl.Peers)
	return curPeers
}

func (pl *peerList) addOrUpdateAnnouncePeer(peer *announcePeer) {
	pl.peerMut.Lock()
	defer pl.peerMut.Unlock()
	
	for _, p := range pl.Peers {
		if p.Equals(peer) {
			
			for _, newAddr := range peer.Addresses {
				found := false 
				for _, oldAddr := range p.Addresses {
					if oldAddr.IP.Equal(newAddr.IP) && oldAddr.Port == newAddr.Port {
						found = true
						break
					}
				}
				if !found {
					p.Addresses = append(p.Addresses, newAddr)
				}
			}
			
			for _, newService := range peer.ServicesOffered {
				found := false 
				for _, oldService := range p.ServicesOffered {
					if oldService == newService {
						found = true
						break
					}
				}
				if !found {
					p.ServicesOffered = append(p.ServicesOffered, newService)
				}
			}
			
			return
		}
	}
	
	pl.Peers = append(pl.Peers, peer)
}

func (pl announcePeerList) toBytes() ([]byte, error) {
	buf, err := json.Marshal(pl)
	if err != nil {
		err = fmt.Errorf("Error from json.Marshal in peerList.toByte(): %v", err)
		log.Error(err)
		return nil, err
	}
	
	payload := make([]byte, protocol_id_size + len(buf))
	
	err = addProtocolId(payload)
	if err != nil {
		err = fmt.Errorf("Can't create announcement, addProtocolId returned error: %v", err)
		log.Error(err)
		return nil, err
	}
	copy(payload[protocol_id_size:], buf)
	
	return payload, nil
}

func announcePeerListFromBytes(buf []byte) (announcePeerList, error) {
	var peers announcePeerList
	err := json.Unmarshal(buf, &peers)
	if err != nil {
		log.Errorf("Error from json.Unmashal in peerListFromBytes: %v", err)
		return nil, err
	}
	return peers, nil
}

func (pl *peerList) getPeer(id uuid.UUID) (peer *announcePeer) {
	for _, p := range pl.Peers {
		if uuid.Equal(p.Id, id) {
			peer = p
			return
		}
	}
	return
}

func (pl *peerList) getRandomPeers(n int, exclude announcePeerList, filter func(p *announcePeer) bool) announcePeerList {
	retPeers := make(announcePeerList, 0)
	curPeers := pl.getPeerList()
	
	for len(retPeers) < n && len(curPeers) > 0 {
		i := peerRand.Intn(len(curPeers))
		p := curPeers[i]
		curPeers = append(curPeers[:i], curPeers[i+1:]...)
		
		if !filter(p) {
			continue
		}
		
		if exclude.Contains(p) {
			continue
		}
		
		retPeers = append(retPeers, p)
	}
	
	return retPeers
}
