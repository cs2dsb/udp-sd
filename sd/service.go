package sd

import (
	"net"
	"fmt"
	"time"
	"strconv"
	"sync"
	"encoding/binary"
	"encoding/json"
	"os"
	"math/rand"
	"errors"
	"golang.org/x/net/ipv4"
	
	
	//"udp-sd/reliable"
	
	log "github.com/Sirupsen/logrus"
)

var (
	ErrNotAPacket = errors.New("Invalid packet")
	ErrNotAnOpCode = errors.New("Invalid opcode")
	ErrMissingPayload = errors.New("Missing payload")
)

type sdOpCode uint8

const (
	sdOpReq sdOpCode = 1 << iota
	sdOpResp
)

type sdRequest struct {
	Hostname string
	ServiceName string
	DesiredServices []string
}

type sdResponse struct {
	Hostname string
	ServiceName string
	ServicePort int
	serviceIp net.IP
}

func (r *sdRequest) ToPacket() (*sdPacket, error) {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("Error from json.Marshal for sdRequest: %v", err)
	}
	
	return makeSdPacket(sdOpReq, buf), nil
}

func (r *sdRequest) String() string {
	if len(r.DesiredServices) > 0 {
		return fmt.Sprintf("'%s' on '%s' is looking for: %v", r.ServiceName, r.Hostname, r.DesiredServices)
	}
	return fmt.Sprintf("%s on %s is looking for any services", r.ServiceName, r.Hostname)
}

func (r *sdResponse) ToPacket() (*sdPacket, error) {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("Error from json.Marshal for sdResponse: %v", err)
	}
	
	return makeSdPacket(sdOpResp, buf), nil
}

func (r *sdResponse) String() string {
	return fmt.Sprintf("%s is offering %v at %v:%d", r.Hostname, r.ServiceName, r.serviceIp, r.ServicePort)	
}


func (p *sdPacket) ToRequest() (*sdRequest, error) {
	if len(p.payload) == 0 {
		return nil, ErrMissingPayload
	}
	
	var req sdRequest
	err := json.Unmarshal(p.payload, &req)
	
	if err != nil {
		return nil, fmt.Errorf("Error from json.Unmarshal for sdRequest: %v", err)
	}
	
	return &req, nil
}

func (p *sdPacket) ToResponse() (*sdResponse, error) {
	if len(p.payload) == 0 {
		return nil, ErrMissingPayload
	}
	
	var resp sdResponse
	err := json.Unmarshal(p.payload, &resp)
	
	if err != nil {
		return nil, fmt.Errorf("Error from json.Unmarshal for sdResponse: %v", err)
	}
	
	return &resp, nil
}


func (o sdOpCode) Serialize() []byte {
	buf := make([]byte, binary.Size(o))
	binary.PutUvarint(buf, uint64(o))
	
	return buf
}

func DeserializeOpCode(buf []byte) (sdOpCode, int, error) {
	minLength := binary.Size(sdOpReq)
	if len(buf) < minLength {
		return 0, 0, ErrNotAnOpCode
	}
	
	v, n := binary.Uvarint(buf)
	return sdOpCode(v), n, nil
}

type sdPacket struct {
	magic []byte
	opCode sdOpCode
	payload []byte
	src *net.UDPAddr
}

func getMagic() []byte {
	return []byte{123,222,199}
}

func makeSdPacket(opcode sdOpCode, payload []byte) *sdPacket {
	p := sdPacket{}
	p.magic = getMagic()
	p.opCode = opcode
	p.payload = payload
	return &p
}

func (p sdPacket) Serialize() []byte {
	
	bufOp := p.opCode.Serialize()
	
	rl := 0
	rl += len(p.magic)
	rl += len(bufOp)
	rl += len(p.payload)
	
	r := make([]byte, rl)
	
	
	n := 0
	//fmt.Printf("r (0): %v\n", r)
	n += copy(r[n:], p.magic)
	//fmt.Printf("r (1): %v\n", r)
	n += copy(r[n:], bufOp)
	//fmt.Printf("r (2): %v\n", r)
	n += copy(r[n:], p.payload)
	//fmt.Printf("Serialized: %v\n", r)
	
	return r
}

func DeserializePacket(p []byte) (sdPacket, error) {
	packet := sdPacket{}
	magic := getMagic()
	
	minLength := len(magic) + binary.Size(sdOpReq)
	
	if len(p) < minLength {
		return packet, ErrNotAPacket
	}
	
	for i, v := range magic {
		if v != p[i] {
			return packet, ErrNotAPacket
		}
	}
	packet.magic = magic
	
	n := len(magic)
	
	op, on, err := DeserializeOpCode(p[n:])
	if err != nil {
		return packet, err
	}
	packet.opCode = op
	n += on
	
	packet.payload = p[n:len(p)]
	
	return packet, nil
	
}

func getViableInterfaces() []*net.Interface {
	allInterfaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	
	selectedInterfaces := make([]*net.Interface, 0)
	
	for _, ifc:= range allInterfaces {
		if ifc.Flags & net.FlagUp == net.FlagUp && ifc.Flags & net.FlagMulticast == net.FlagMulticast {
			selectedInterfaces = append(selectedInterfaces, &ifc)
		}
	}
	
	return selectedInterfaces
}

const (
	advertising_group = "239.255.255.92"
	advertising_port = "4543"
)

type Service struct {
	Name string
	online bool
	RequiredServices []string
	serviceReferences map[string][]*net.UDPAddr
	port int
	connection *ipv4.PacketConn
	adConnection *ipv4.PacketConn
	adAddress *net.UDPAddr
	hostname string
	interfaces []*net.Interface
}

func (s *Service) BringOnline() error {
	if s.online {
		return errors.New("Service is already online")
	}	
			
	c, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		return fmt.Errorf("Error listening to service port: %v", err)
	}
	
	p := ipv4.NewPacketConn(c)
	s.port = c.LocalAddr().(*net.UDPAddr).Port
	s.online = true
	s.connection = p
	s.serviceReferences = make(map[string][]*net.UDPAddr)
	
	hostname, _ := os.Hostname()
	s.hostname = hostname
	
	interfaces := getViableInterfaces()
	s.interfaces = interfaces
	
	fmt.Printf("Service '%s' will be on port %d\n", s.Name, s.port)
	
	
	var initErr error
	var initWG sync.WaitGroup
	initWG.Add(1)
	
	go func() {		
		adGroup := net.ParseIP(advertising_group)
		adPort64, _ := strconv.ParseInt(advertising_port, 10, 64)
		adPort := int(adPort64)	
		s.adAddress = &net.UDPAddr{IP: adGroup, Port: adPort}
		
		c, err = net.ListenUDP("udp4", s.adAddress)
		if err != nil {
			initErr = fmt.Errorf("Error listening to advertising port: %v", err)
			initWG.Done()
			return
		}
		defer c.Close()

		s.adConnection = ipv4.NewPacketConn(c)
		defer s.adConnection.Close()
		
		for _, ifc := range interfaces {
			err := s.adConnection.JoinGroup(ifc, &net.UDPAddr{IP: adGroup})
			if err != nil {
				initErr = fmt.Errorf("Error joining advertising group: %v", err)
				initWG.Done()
				return	
			}
		}
		
		
		//Here we goooo
		initWG.Done()
		
		go func() {
			wait := time.Millisecond * 0
			ok := false
			for s.online && (ok == false) {
				time.Sleep(wait)
				if wait < time.Millisecond * 500 {
				//	wait += time.Second
				}
				
				var missing []string
				ok, missing = s.requirementsSatisfied()
				if !ok {
					fmt.Printf("Service '%s' missing %d services: %v\n", s.Name, len(missing), missing)					
					
					if len(missing) > 20 {
						newList := make([]string, 20)
						for i := 0; i < 20; i++ {
							j := rand.Intn(len(missing))
							newList[i] = missing[j]
							missing = append(missing[:j], missing[j+1:]...)							
						}
						missing = newList
					}
					req := sdRequest {
						Hostname: s.hostname,
						ServiceName: s.Name,
						DesiredServices: missing,
					}
					
					jitter := time.Millisecond * time.Duration(500.0 * rand.Float64() + 500.0)
					//fmt.Printf("Jitter: %v\n", jitter)
					time.Sleep(jitter)
					
					packet, err := req.ToPacket()
					if err != nil {
						log.Errorf("Error serializing request packet: %v", err)
						continue
					}
					
					msg := packet.Serialize()
					s.connection.SetTOS(0x0)
					s.connection.SetTTL(16)
					//adPacketCon.SetTOS(0x0)
					//adPacketCon.SetTTL(16)
					for _, ifc := range s.interfaces {
						//err := adPacketCon.SetMulticastInterface(ifc)
						err := s.connection.SetMulticastInterface(ifc)
						if err != nil {
							log.Errorf("Error setting multicast interface: %v", err)
							continue
						}
				
						//adPacketCon.SetMulticastTTL(2)
						s.connection.SetMulticastTTL(2)
						//_, err = adPacketCon.WriteTo(msg, nil, adListenAddress)
						_, err = s.connection.WriteTo(msg, nil, s.adAddress)
						if err != nil {
							log.Errorf("Error writing request to multicast interface: %v", err)
							continue
						}
					}
				}
			}
		}()
		
		s.listenToConnections()		
	}()
	

	initWG.Wait()
	if initErr != nil {
		return initErr
	}
	
	return err
}

func (s *Service) listenToConnections() {
	connections := []*ipv4.PacketConn {
		s.adConnection,
		s.connection,
	}
	
	var wg sync.WaitGroup	
	wg.Add(len(connections))
	
	for _, c := range connections {
		go func(conn *ipv4.PacketConn) {
			incomingBuf := make([]byte, 64*1024)
			for s.online {
				n, _, src, err := conn.ReadFrom(incomingBuf)
				if err != nil {
					log.Errorf("Error reading from connection (%v): %v", conn, err)
					continue
				}
				packet, err := DeserializePacket(incomingBuf[0:n])
				if err != nil {
					continue
				}
				packet.src = src.(*net.UDPAddr)
				err = s.handlePacket(&packet)
				if err != nil {
					log.Errorf("Error handling packet: %v", err)
				}
			}
			wg.Done()
		}(c)
	}	
	
	wg.Wait()
}

func (s *Service) handlePacket(p *sdPacket) error {	
	if p.opCode & sdOpReq == sdOpReq {
		req, err := p.ToRequest()
		if err != nil {			
			return fmt.Errorf("Error handling request packet: %v\n", err)
		}
		if req.Hostname == s.hostname && req.ServiceName == s.Name {
			return nil
		}
		shouldReply := false
		ok, _ := s.requirementsSatisfied()
		if ok == true{
			if len(req.DesiredServices) == 0 {
				shouldReply = true
			} else {
				for _, rs := range req.DesiredServices {
					if rs == s.Name {
						shouldReply = true
						break
					}		
				}
			}
		}
		if shouldReply {
			resp := sdResponse{
				Hostname: s.hostname,
				ServiceName: s.Name,
				ServicePort: s.port,
			}
			packet, err := resp.ToPacket()
			if err != nil {
				return fmt.Errorf("Error serializing response: %v\n", err)				
			}
			
			buf := packet.Serialize()
			_, err = s.adConnection.WriteTo(buf, nil, p.src)
			if err != nil {
				log.Errorf("Error sending sdResponse: %v", err)
			}
		}
	}
	if p.opCode & sdOpResp == sdOpResp {
		resp, _ := p.ToResponse()
		
		if resp.Hostname == s.hostname && resp.ServiceName == s.Name {
			return nil
		}
		
		resp.serviceIp = p.src.IP
		s.processSdResponse(resp)
	}
	
	return nil
}

func (s *Service) TakeOffline() error {
	return nil
}

func (s *Service) processSdResponse(resp *sdResponse) {
	used := false
	for _, n := range s.RequiredServices {
		if n == resp.ServiceName {
			address := &net.UDPAddr{ 
				IP: resp.serviceIp, 
				Port: resp.ServicePort,
			}
			
			list, ok := s.serviceReferences[n]			
			if ok == false {
				list = make([]*net.UDPAddr, 1)
				list[0] = address
				used = true
			} else {
				found := false
				for _, knownAddr := range list {
					if address.IP.Equal(knownAddr.IP) && address.Port == knownAddr.Port {
						found = true
						break
					}
				}
				if found == false {
					list = append(list, address)					
					used = true
				}
			}
		
			if used == true {
				s.serviceReferences[n] = list
				log.Infof("Service '%s' used service reference to '%s': %v", s.Name, resp.ServiceName, address)
			}			
		}
	}
}

func (s *Service) requirementsSatisfied() (bool, []string) {
	ok := true
	missing := make([]string, 0)
	for _, r := range s.RequiredServices {
		if len(s.serviceReferences[r]) == 0 {
			ok = false
			missing = append(missing, r)			
		}
	}
	
	return ok, missing
}

// Wait for all required services to be identified
func (s *Service) WaitForOnline() error {
	if s.online == false {
		return fmt.Errorf("Service isn't trying to come online")
	}
	
	wait := time.Millisecond * 200
	ok, _ := s.requirementsSatisfied()
	for ok == false {		
		ok, _ = s.requirementsSatisfied()
		time.Sleep(wait)
		/*if wait < time.Second * 1 {
			wait *= 2
		}*/
	}
	
	return nil
}

func (s *Service) IsOnline() bool {
	return s.online
}































