package reliable

import (
	"math/rand"
	"fmt"
	"encoding/binary"
	"time"
	"net"
	"sync"
	"strconv"
	"testing"
	"github.com/cs2dsb/udp-sd/util"
)

func TestEstablishConnection(t *testing.T) {
	r, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	r.Close()
}

func TestEstablishMultipleConnections(t *testing.T) {
	r1, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r1 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	r2, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error on 2nd invocation: %q", err)
	}
	if r2 == nil {
		t.Errorf("NewReliableConnection() returned nil connection on 2nd invocation")
	}
	
	r1.Close()
	r2.Close()
}

func TestConnectPeer(t *testing.T) {
	r1, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r1 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	r2, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r2 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	p := r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		t.Errorf("ConnectPeer() returned nil peer")
	}
	
	isAlive := p.IsAlive(time.Millisecond * 2000)
	if !isAlive {
		t.Errorf("peer.IsAlive(2000ms) returned false")
	}	
	
	r1.Close()
	r2.Close()
}

func TestPacketEncodeDecode(t *testing.T) {
	p := packet{
		//Note this peer isn't valid for communication, use .ConnectPeer for real peers
		Peer: &peer{Address: &net.UDPAddr{IP: net.IPv4(123,245,178,111), Port: 12349}},
		OpCode: opData,
		Payload: []byte{123,123,123,111,110,99,222},
		Seq: 453432,
		Ack: 34564,
		AckBitfield: 99999,
	}
	
	ip := encodedPacket {
		Peer: p.Peer,
		Payload: p.toBytes(),
	}
	
	p2, err := ip.toOutgoingPacket()
	if err != nil {
		t.Errorf("incomingPacket.toOutgoingPacket() returned an error: %q", err)
	}
	if p2 == nil {
		t.Errorf("incomingPacket.toOutgoingPacket() returned nil outgoing packet")
	}
	
	if !(p.Peer.Address.IP.Equal(p2.Peer.Address.IP) && p.Peer.Address.Port == p2.Peer.Address.Port) {
		t.Errorf("incomingPacket.toOutgoingPacket() returned a different Address. Got %v:%d, expected %v:%d", p2.Peer.Address.IP, p2.Peer.Address.Port, p.Peer.Address.IP, p.Peer.Address.Port)
	}
	
	if p.OpCode != p2.OpCode {
		t.Errorf("incomingPacket.toOutgoingPacket() returned a different OpCode. Got %d, expected %d", p2.OpCode, p.OpCode)
	}
	
	if p.Seq != p2.Seq {
		t.Errorf("incomingPacket.toOutgoingPacket() returned a different Seq. Got %d, expected %d", p2.Seq, p.Seq)
	}
	
	if p.Ack != p2.Ack {
		t.Errorf("incomingPacket.toOutgoingPacket() returned a different ack. Got %d, expected %d", p2.Ack, p.Ack)
	}
	
	if p.AckBitfield != p2.AckBitfield {
		t.Errorf("incomingPacket.toOutgoingPacket() returned a different AckBitfield. Got %d, expected %d", p2.AckBitfield, p.AckBitfield)
	}
	
	if p2.Payload == nil {
		t.Errorf("incomingPacket.toOutgoingPacket() returned a different payload. Got nil")
	}
	
	pp := p.Payload
	p2p := p2.Payload
	
	for i, b := range pp {
		b2 := p2p[i]
		if b != b2 {
			t.Errorf("incomingPacket.toOutgoingPacket() returned a different payload. Got %d, expected %d (in byte %d)", b2, b, i)			
		}
	}
}

func TestPacketKeepaliveIncreasesSeq(t *testing.T) {
	r1, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r1 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	r2, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r2 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	p := r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		t.Errorf("ConnectPeer() returned nil peer")
	}
	
	s := p.Seq
	r1.sendKeepaliveToPeer(p)
	
	if p.Seq < s + 1 {
		t.Errorf("Peer's Seq didn't increment after send keepalive")
	}
	
	r1.Close()
	r2.Close()
}

func TestPacketGetsAcked(t *testing.T) {
	r1, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r1 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	r2, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r2 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	p := r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		t.Errorf("ConnectPeer() returned nil peer")
	}
		
	packet := newPacketWithRetries(p, nil, 0, opPing)
	r1.queuePacketForSend(packet)
	
	ok := util.WaitUntilTrue(func() bool {
		return p.GetPacketIsPacketUnacked(packet)
	}, time.Millisecond * 2000)
	
	if !ok {
		t.Errorf("Queued packet missing from unacked packets")
	}
	
	ok = util.WaitUntilTrue(func() bool {
		return !p.GetPacketIsPacketUnacked(packet)
	}, time.Millisecond * 2000)
	
	if !ok {
		t.Errorf("Packet not acked after 2 seconds")
	}
	r1.Close()
	r2.Close()
}

func TestKeepAliveGetsSent(t *testing.T) {
	r1, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r1 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	r2, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r2 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	p := r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		t.Errorf("ConnectPeer() returned nil peer")
	}
		
	ok := p.IsAlive(p.packetTimeout * 2)
	if !ok {
		t.Errorf("Packet not acked after 2 seconds")
	}
	r1.Close()
	r2.Close()
}

func parseBitfield(t *testing.T, bitfield string) uint32 {
	i, err := strconv.ParseUint(bitfield, 2, 32)
	if err != nil {
		t.Errorf("Invalid bitfield specification")
	}
	return uint32(i)
}

func compareAckLists(expected, actual []uint32) error {
	for i := len(expected) - 1; i >= 0; i-- {
		ev := expected[i]
		for j := len(actual) - 1; j >= 0; j-- {
			av := actual[j]
			if ev == av {
				actual = append(actual[:j], actual[j+1:]...)
				expected = append(expected[:i], expected[i+1:]...)	
				break
			}
		}
	}
	ret := ""
	if len(expected) > 0 {
		ret += fmt.Sprintf("Expected Ack values missing (%v) from actual", expected)
	}
	if len(actual) > 0 {
		if len(ret) > 0 {
			ret += "\n"
		}
		ret += fmt.Sprintf("Unexpected Ack values in actual (%v)", actual)
	}
	
	if len(ret) > 0 {
		return fmt.Errorf(ret)
	}
	return nil
}

func TestAckList(t *testing.T) {
	op := &packet{}
	op.Ack = 97	
	
	op.AckBitfield = uint32(parseBitfield(t, "00000000000000000000000000000000"))
	err := compareAckLists([]uint32{op.Ack}, op.ackList())
	if err != nil {
		t.Error(err)
	}
	
	op.AckBitfield = uint32(parseBitfield(t, "00000000000000000000000000000001"))
	err = compareAckLists([]uint32{op.Ack, op.Ack - 1}, op.ackList())
	if err != nil {
		t.Error(err)
	}
	
	op.AckBitfield = uint32(parseBitfield(t, "00000000000000000000000000000010"))
	err = compareAckLists([]uint32{op.Ack, op.Ack - 2}, op.ackList())
	if err != nil {
		t.Error(err)
	}
	
	op.AckBitfield = uint32(parseBitfield(t, "00000000000000000000000000000100"))
	err = compareAckLists([]uint32{op.Ack, op.Ack - 3}, op.ackList())
	if err != nil {
		t.Error(err)
	}
	
	op.AckBitfield = uint32(parseBitfield(t, "00000000000000000000000000000111"))
	err = compareAckLists([]uint32{op.Ack, op.Ack - 3, op.Ack - 2, op.Ack - 1}, op.ackList())
	if err != nil {
		t.Error(err)
	}
	
	op.AckBitfield = uint32(parseBitfield(t, "10000000000000000000000000000111"))
	err = compareAckLists([]uint32{op.Ack, op.Ack - 32, op.Ack - 3, op.Ack - 2, op.Ack - 1}, op.ackList())
	if err != nil {
		t.Error(err)
	}
	
	op.AckBitfield = uint32(parseBitfield(t, "11111111111111111111111111111111"))
	list := []uint32{op.Ack}
	for i := uint32(1); i <= 32; i++ {
		list = append(list, op.Ack - i)
	}
	err = compareAckLists(list, op.ackList())
	if err != nil {
		t.Error(err)
	}
}

func TestPeerUpdateRemoteSeq(t *testing.T) {
	p := &peer{}
	
	s := p.RemoteSeq
	p.updateRemoteSeq(s+1)
	s2 := p.RemoteSeq
	if s + 1 != s2 {
		t.Errorf("Expected new remote seq of %d but got %d after p.updateRemoteSeq", s + 1, s2)
	}
	
	s = p.RemoteSeq
	p.updateRemoteSeq(s+1)
	s2 = p.RemoteSeq
	if s + 1 != s2 {
		t.Errorf("Expected new remote seq of %d but got %d after p.updateRemoteSeq", s + 1, s2)
	}
	
	if p.AckBitfield & 1 != 1 {
		t.Errorf("Expected AckBitfield to end in 1 but got 0")
	}	
	
	p.updateRemoteSeq(s2+2)
	s3 := p.RemoteSeq
	if s2 + 2 != s3 {
		t.Errorf("Expected new remote seq of %d but got %d after p.updateRemoteSeq", s2 + 2, s3)
	}
	
	if p.AckBitfield & 1 != 0 {
		t.Errorf("Expected AckBitfield to end in 0 but got 1")
	}
	
	s = p.RemoteSeq
	p.updateRemoteSeq(s+1)
	s2 = p.RemoteSeq
	if s + 1 != s2 {
		t.Errorf("Expected new remote seq of %d but got %d after p.updateRemoteSeq", s + 1, s2)
	}
	
	if p.AckBitfield & 1 != 1 {
		t.Errorf("Expected AckBitfield to end in 1 but got 0")
	}	
	
	p.updateRemoteSeq(s2)
	s4 := p.RemoteSeq
	if s2 != s4 {
		t.Errorf("Expected new remote seq of %d but got %d after p.updateRemoteSeq", s3, s4)
	}
		
	p.updateRemoteSeq(s)
	s2 = p.RemoteSeq
	if s4 != s2 {
		t.Errorf("Expected new remote seq of %d but got %d after p.updateRemoteSeq", s4, s2)
	}
	
	p.AckBitfield = parseBitfield(t, "00000000000000000000000000010101")
	p.updateRemoteSeq(p.RemoteSeq - 2)
	expected := parseBitfield(t, "00000000000000000000000000010111")
	if p.AckBitfield != expected {
		t.Errorf("Expected bitfield %s, got %s", strconv.FormatUint(uint64(expected), 2), strconv.FormatUint(uint64(p.AckBitfield), 2))
	}
	
	p.updateRemoteSeq(p.RemoteSeq - 4)
	expected = parseBitfield(t, "00000000000000000000000000011111")
	if p.AckBitfield != expected {
		t.Errorf("Expected bitfield %s, got %s", strconv.FormatUint(uint64(expected), 2), strconv.FormatUint(uint64(p.AckBitfield), 2))
	}	
}

func mergeAckLists(a, b []uint32) []uint32 {
	for _, v := range b {
		found := false
		for _, v2 := range a {
			if v == v2 {
				found = true
				break
			}
		}
		if !found {
			a = append(a, v)
		}
	}
	
	return a
}

func TestAckListOutOfOrder(t *testing.T) {
	p := NewPeer(nil, &dummyCon{}) 
	pkt := &packet{}
	
	if p.Seq != 1 {
		t.Fatalf("Unexpected initial peer Seq of %d, expected 1", p.Seq)
	}
	
	expectedAcks := []uint32{1, 2, 4, 5, 7, 8, 10}
	for _, s := range expectedAcks {
		p.updateRemoteSeq(s)
	}
	pkt.Ack = p.RemoteSeq
	pkt.AckBitfield = p.AckBitfield
	
	err := compareAckLists(expectedAcks, pkt.ackList())
	if err != nil {
		t.Fatal(err)
	}
	
	oooAcks := []uint32{6, 9}
	for _, s := range oooAcks {
		p.updateRemoteSeq(s)
	}
	expectedAcks = mergeAckLists(expectedAcks, oooAcks)
	pkt.Ack = p.RemoteSeq
	pkt.AckBitfield = p.AckBitfield
	
	err = compareAckLists(expectedAcks, pkt.ackList())
	if err != nil {
		t.Fatal(err)
	}
	
	oooAcks = []uint32{11, 3, 12, 4, 11, 13}
	for _, s := range oooAcks {
		p.updateRemoteSeq(s)	
	}
	expectedAcks = mergeAckLists(expectedAcks, oooAcks)
	pkt.Ack = p.RemoteSeq
	pkt.AckBitfield = p.AckBitfield
	
	err = compareAckLists(expectedAcks, pkt.ackList())
	if err != nil {
		t.Fatal(err)
	}

	p.AckBitfield = 0
	p.RemoteSeq = 0
	
	//The rest come from some random logged output from other tests (was a case that was failing)
	oooAcks = []uint32{
		13,
		14,
		15,
		16,
		17,
		19,
		20,
		21,
		22,
		23,
		24,
		25,
		26,
		27,
		28,
		29,
		30,
		31,
		32,
		33,
		34,
		35,
		36,
		37,
	}	

	expectedAcks = []uint32{ 12, 10, 9, 7, 6 }
	
	binTo32 := func(str string) uint32 {
		v, err := strconv.ParseUint(str, 2, 32)
		if err != nil {
			panic(err)
		}
		return uint32(v)
	}
	oooExpBefore := []uint32{
		binTo32("10110110110"),
		binTo32("101101101101"),
		binTo32("1011011011011"),
		binTo32("10110110110111"),
		binTo32("101101101101111"),
		binTo32("1011011011011111"),
		binTo32("101101101101111110"),
		binTo32("1011011011011111101"),
		binTo32("10110110110111111011"),
		binTo32("101101101101111110111"),
		binTo32("1011011011011111101111"),
		binTo32("10110110110111111011111"),
		binTo32("101101101101111110111111"),
		binTo32("1011011011011111101111111"),
		binTo32("10110110110111111011111111"),
		binTo32("101101101101111110111111111"),
		binTo32("1011011011011111101111111111"),
		binTo32("10110110110111111011111111111"),
		binTo32("101101101101111110111111111111"),
		binTo32("1011011011011111101111111111111"),
		binTo32("10110110110111111011111111111111"),
		binTo32("1101101101111110111111111111111"),
		binTo32("11011011011111101111111111111111"),
		binTo32("10110110111111011111111111111111"),
	}
	
	oooExpAfter := []uint32{
		binTo32("101101101101"),
		binTo32("1011011011011"),
		binTo32("10110110110111"),
		binTo32("101101101101111"),
		binTo32("1011011011011111"),
		binTo32("101101101101111110"),
		binTo32("1011011011011111101"),
		binTo32("10110110110111111011"),
		binTo32("101101101101111110111"),
		binTo32("1011011011011111101111"),
		binTo32("10110110110111111011111"),
		binTo32("101101101101111110111111"),
		binTo32("1011011011011111101111111"),
		binTo32("10110110110111111011111111"),
		binTo32("101101101101111110111111111"),
		binTo32("1011011011011111101111111111"),
		binTo32("10110110110111111011111111111"),
		binTo32("101101101101111110111111111111"),
		binTo32("1011011011011111101111111111111"),
		binTo32("10110110110111111011111111111111"),
		binTo32("1101101101111110111111111111111"),
		binTo32("11011011011111101111111111111111"),
		binTo32("10110110111111011111111111111111"),
		binTo32("1101101111110111111111111111111"),
	}
	
	p.AckBitfield = oooExpBefore[0]
	p.RemoteSeq = 12
	
	for i, s := range oooAcks {
		bfE := oooExpBefore[i]
		bfA := oooExpAfter[i]
		if p.AckBitfield != bfE {
			t.Fatalf("Ack bitfield before was expected to be %d but was %d instead", bfE, p.AckBitfield)			
		}
		p.updateRemoteSeq(s)	
		if p.AckBitfield != bfA {
			t.Fatalf("%d: Ack bitfield after was expected to be %d but was %d instead", i, bfA, p.AckBitfield)
		}
	}
	expectedAcks = mergeAckLists(expectedAcks, oooAcks)
	pkt.Ack = p.RemoteSeq
	pkt.AckBitfield = p.AckBitfield
	
	err = compareAckLists(expectedAcks, pkt.ackList())
	if err != nil {
		t.Fatal(err)
	}
}

func TestAckListFalsePositives(t *testing.T) {
	p := NewPeer(nil, &dummyCon{})
	pkt := &packet{}
	
	if p.Seq != 1 {
		t.Fatalf("Unexpected initial peer Seq of %d, expected 1", p.Seq)
	}
	
	expectedAcks := make([]uint32, 33)
	
	for seq := uint32(1); seq <= 1000; seq ++ {
		r := rand.Intn(10)
		if r == 0 {			
			//Some lost packets
			r = rand.Intn(3)
			
			for i := uint32(0); i < uint32(r) + 2; i++ {
				expectedAcks[(seq + i - 1) % 33] = 0
			}
			seq += uint32(r)
			continue
		} 
			
		expectedAcks[(seq - 1) % 33] = seq
		//fmt.Printf("%v\n", expectedAcks)
		
		p.updateRemoteSeq(seq)
		
		pkt.Ack = p.RemoteSeq
		pkt.AckBitfield = p.AckBitfield

		list := make([]uint32, 0)
		for _, v := range expectedAcks {
			if v != 0 {
				list = append(list, v)
			}
		}	
		err := compareAckLists(list, pkt.ackList())
		if err != nil {
			t.Error(err)
			fmt.Println(err)
		}
	}
}

func TestDeadPeerGetsDropped(t *testing.T) {
	r1, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r1 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	r1.connectionTimeout = time.Millisecond * 1000
	r1.packetTimeout = time.Millisecond * 300
	
	r2, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
	if r2 == nil {
		t.Errorf("NewReliableConnection() returned nil connection")
	}
	
	r1.EnableKeepalive(false)
	r2.EnableKeepalive(false)
	
	p := r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		t.Errorf("ConnectPeer() returned nil peer")
	}
	
	r1.sendKeepaliveToPeer(p)
	
	isAlive := p.IsAlive(time.Millisecond * 2000)
	if !isAlive {
		t.Errorf("peer.IsAlive(2000ms) returned false")
	}
	
	r2.Close()
	
	wait := r1.connectionTimeout * 2
	w := time.Millisecond * 500
	found := false
	for wait > 0 {
		found = false
		enumReq := r1.getEnumeratePeersChan()
		for p2 := range enumReq.RespChan {
			if p2 == p {
				found = true
				close(enumReq.AbortChan)
				break
			}
		}
		
		if found {
			r1.sendKeepaliveToPeer(p)
			wait -= w
			time.Sleep(w)
		} else {
			wait = 0
		}
	}
	if found {
		t.Errorf("peer was not removed after 2 * connection timeout")
	}
	r1.Close()
}


type dummyCon struct {
	sendPacketChan chan *packet
}

func (c *dummyCon) sendPacket(p *packet) {
	c.sendPacketChan <- p
}

func (c *dummyCon) sendKeepaliveToPeer(p *peer) {
	
}

func (c *dummyCon) getConnectionTimeout() time.Duration {
	return time.Second
}

func (c *dummyCon) getPacketTimeout() time.Duration {
	return time.Second
}

func (c *dummyCon) FindOrAddPeer(address *net.UDPAddr) *peer {
	return nil
}

func (c *dummyCon) GetIncomingPacketChannel() chan *packet {
	return nil
}

func (c *dummyCon) GetListenAddresses() []*net.UDPAddr {
	return nil
}

func (c *dummyCon) GetPeerList() []*peer {
	return nil
}

func (c *dummyCon) IsOnline() bool {
	return true
}

func (c *dummyCon) GetSweepInterval() time.Duration {
	return time.Second
}

func TestPeerDispatchesPackets(t *testing.T) {
	c := &dummyCon{
		sendPacketChan: make(chan *packet),
	}
	p := NewPeer(nil, c)
	
	n := 10
	go func() {
		for i := 0; i < n; i++ {
			pkt := newPacketWithRetries(p, nil, 0, opData)
			p.SendPacket(pkt)
		}
	}()
	
	got := 0
	go func() {
		for got < n {			
			p.dispatch(time.Now())
		}
	}()
	
	go func() {
		ok := util.WaitUntilTrue(func() bool {
			return got == n
		}, time.Second * 10)
		if !ok {
			t.Errorf("Peer hasn't dispatched packets after 10 seconds")
			close(c.sendPacketChan)
		}
	}()
	
	for got < n {
		<- c.sendPacketChan		
		got ++
	}
	p.Close()
}

func TestPacketRetrying(t *testing.T) {
	r1, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)		
	}
	
	r2 := &con{}	
	
	wait := r1.packetTimeout
	
	lossyHandler := func (incomingPackets chan *encodedPacket) {
		innerChan := make(chan *encodedPacket)
		defer close(innerChan)
		go r2.handlePackets(innerChan)
		
		whoops := 2
		for p := range incomingPackets {
			pkt, err := p.toOutgoingPacket()
			if err != nil {
				t.Errorf("Error converting to outgoingPacket: %v", err)
				return
			}
			if pkt.OpCode & opData == opData{
				i, _ := binary.Uvarint(pkt.Payload)
				whoops ++
				if whoops % 3 == 0 {
					fmt.Printf("lossy handler dropping %d: %v\n", i, pkt)
					wait += r2.packetTimeout
					continue
				}
				t.Logf("lossy handler passing on %d: %v", i, pkt)
			}
			innerChan <- p
		}
		
		fmt.Println("lossy handler exiting")
	}
	
	_, err = newReliableConnectionWithListenerHandlers(r2, lossyHandler, r2.dispatchPackets)
	if err != nil {
		t.Errorf("newReliableConnectionWithListenerHandlers() returned error: %q", err)
	}
	
	p := r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		t.Errorf("ConnectPeer() returned nil peer")
	}
	
	isAlive := p.IsAlive(time.Millisecond * 2000)
	if !isAlive {
		t.Errorf("peer.IsAlive(2000ms) returned false")
	}
	
	n := uint32(10)
	
	received := make([]*packet, int(n))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for pkt := range r2.IncomingPackets {	
			if pkt.OpCode & opData != opData {
				continue
			}	
			i, _ := binary.Uvarint(pkt.Payload)
			received[int(i)] = pkt
		}
		return
	}()
	
	for i := uint32(0); i < n; i++ {
		buf := make([]byte,binary.Size(i))
		binary.PutUvarint(buf, uint64(i))
		
		packet := newPacketWithRetries(p, buf, 30, opData)
		r1.queuePacketForSend(packet)
		
		time.Sleep(tight_loop_delay)
	}
	
	ok := util.WaitUntilTrueWithVariableWait(func () bool {		
		for _, v := range received {
			if v == nil {
				return false
			}
		}
		return true
	}, func() time.Duration {
		return wait
	})
	
	if !ok {
		t.Errorf("We didn't receive our packets after %v", wait)
	}
	
	r1.Close()
	r2.Close()	
	wg.Wait()
	
	for i := uint32(0); i < n; i++ {
		if received[i] == nil {
			t.Errorf("Packet %d missing", i)
		}
	}
}

func TestPeerSendData(t *testing.T) {
	r1, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)
	}
		
	r2, err := NewReliableConnection()
	if err != nil {
		t.Errorf("NewReliableConnection() returned error: %q", err)		
	}
	
	p := r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		t.Errorf("ConnectPeer() returned nil peer")
	}
	
	isAlive := p.IsAlive(time.Millisecond * 2000)
	if !isAlive {
		t.Errorf("peer.IsAlive(2000ms) returned false")
	}
	
	
	var pkt *packet
	pkt = nil
	var wg sync.WaitGroup
	wg.Add(1)
	
	go func() {
		defer wg.Done()
		wait := default_connection_timeout		
		for {
			select {
				case pkt = <- r2.IncomingPackets:
					if pkt.OpCode & opData == opData {
						return
					}
				case <- time.After(wait):
					pkt = nil
					return
			}
		}
	}()
	
	time.Sleep(time.Second)
	buf := []byte("Hello I'm some data")
	p.SendData(buf)
	
	wg.Wait()

	if pkt == nil {
		t.Errorf("Failed to receive a packet")		
	} else if pkt != nil && len(pkt.Payload) != len(buf) {
		t.Errorf("Received packet length was %d, expected %d", len(pkt.Payload), len(buf))
	} else {
		for i, v := range buf {
			v2 := pkt.Payload[i]
			if v != v2 {
				t.Errorf("Received packet different at byte %d, got %d, expected %d", i, v2, v)
			}
		}
	}
	
	r1.Close()
	r2.Close()	
}

func TestPacketsDroppedAfterRetriesExpired(t *testing.T) {
	r1, err := NewReliableConnection()
	if err != nil {
		t.Fatalf("NewReliableConnection() returned error: %q", err)		
	}
	
	r2 := &con{}	
	
	gotPacketCount := uint32(0)
	buf := []byte("Hello I'm some data")
	
	lossyHandler := func (incomingPackets chan *encodedPacket) {
		innerChan := make(chan *encodedPacket)
		defer close(innerChan)
		go r2.handlePackets(innerChan)
		
		for p := range incomingPackets {
			pkt, err := p.toOutgoingPacket()
			if err != nil {
				t.Fatalf("Error converting to outgoingPacket: %v", err)
				return
			}
			if pkt.OpCode & opData == opData && len(buf) == len(pkt.Payload){
				same := true
				for i, v := range buf {
					v2 := pkt.Payload[i]
					if v != v2 {
						same = false
						break
					}
				}
				if same {
					gotPacketCount++
					continue
				}
			}
			innerChan <- p
		}
	}
	
	_, err = newReliableConnectionWithListenerHandlers(r2, lossyHandler, r2.dispatchPackets)
	if err != nil {
		t.Fatalf("newReliableConnectionWithListenerHandlers() returned error: %q", err)
	}
	
	r1.packetTimeout = time.Millisecond * 200
	r2.packetTimeout = time.Millisecond * 200
	
	p := r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: r2.Port })
	if p == nil {
		t.Fatalf("ConnectPeer() returned nil peer")
	}
	
	isAlive := p.IsAlive(time.Millisecond * 2000)
	if !isAlive {
		t.Fatalf("peer.IsAlive(2000ms) returned false")
	}
	
	retries := uint32(4)
	packet := newPacketWithRetries(p, buf, retries, opData)
	r1.queuePacketForSend(packet)
	
	ok := util.WaitUntilTrue(func () bool {
		return len(p.GetUnackedPacketList()) == 0 || gotPacketCount >= retries
	}, p.connectionTimeout * 2)
	
	if !ok {
		t.Fatalf("peer had %d unacked packets after 2 * connection timeout and we didn't get the packet we were expecting the correct number of times", len(p.GetUnackedPacketList()))
	}
	
	if gotPacketCount != retries {
		t.Fatalf("We got the packet we wanted %d times, expected %d", gotPacketCount, retries)
	}
	
	time.Sleep(default_packet_timeout)
	r1.Close()
	r2.Close()	
	
}

var garbage interface{}
func BenchmarkPacketToBytes(b *testing.B) {	
	p := NewPeer(nil, &dummyCon{})
	pkt := newPacket(p, []byte("hello hello hello"), opData)
	
	var bytes []byte
	
	for i := 0; i < b.N; i++ {
		bytes = pkt.toBytes()
	}
	
	garbage = bytes
}

func BenchmarkPacketFromBytes(b *testing.B) {	
	p := NewPeer(nil, &dummyCon{})
	pkt := newPacket(p, []byte("hello hello hello"), opData)
	
	ep := encodedPacket{ 
		Peer: p,
		Payload: pkt.toBytes(),
	}
	
	for i := 0; i < b.N; i++ {
		pkt, _ = ep.toOutgoingPacket()
	}
	
	garbage = pkt
}

func BenchmarkFindPeer(b *testing.B) {
	r1, _ := NewReliableConnection()
	for i := 0; i < b.N; i++ {
		garbage = r1.FindOrAddPeer(&net.UDPAddr{IP: net.IPv4(127,0,0,1), Port: 9999 })
	}
}

func TestPeerDispatchesPacketQuickly(t *testing.T) {
	p := NewPeer(nil, &dummyCon{})
	
	var endTime time.Time
	var wg sync.WaitGroup
	wg.Add(1)
	
	myPacket := newPacket(p, []byte("fdjkljfkldjlfkd"), opData)
	
	go func() {
		defer wg.Done()
		
		select {
			case <- time.After(default_connection_timeout):
				t.Fatalf("Didn't receive anything within connection timout")
				return
			case pkt := <- p.outgoingPackets:
				if string(myPacket.Payload) != string(pkt.Payload) {
					t.Fatalf("Received packet had a different payload. Got %v, expected %v", pkt.Payload, myPacket.Payload)
					return
				} else {
					t.Logf("Got packet: %v", pkt)
				}
		}
		
		endTime = time.Now()
	}()
	
	
	startTime := time.Now()
	p.SendPacket(myPacket)
	wg.Wait()
	
	diff := endTime.Sub(startTime)
	t.Logf("Dispatch took %v", diff)
	
	if diff > time.Millisecond * 10 {
		t.Fatalf("Dispatch took over 100ms")
	}
}