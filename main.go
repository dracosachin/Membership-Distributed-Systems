package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// Message types
const (
	JOIN      = "JOIN"
	REQ       = "REQ"
	OK        = "OK"
	NEWVIEW   = "NEWVIEW"
	HEARTBEAT = "HEARTBEAT"
)

// Message structure
type Message struct {
	Type                  string
	RequestId             int
	ViewId                int
	PeerId                int
	MembershipOp          string
	UpdatedMembershipList []int          // For NEWVIEW message
	UpdatedHostnames      map[int]string // Map of peer IDs to hostnames
}

// Peer struct to represent each peer
type Peer struct {
	id             int
	viewId         int
	membershipList []int
	isLeader       bool
	leaderAddr     string
	hostname       string
	hostnames      map[int]string // Map of peer IDs to hostnames
	hostIPs        map[int]string // Map of peer IDs to IP addresses
	mutex          sync.Mutex
	listener       net.Listener
	acks           map[int]int       // Tracks acknowledgments for each request ID
	operations     map[int]Message   // Stores the pending operations
	lastHeartbeat  map[int]time.Time // Last heartbeat time for each peer
	udpConn        *net.UDPConn      // UDP connection for heartbeats
	crashAfter     int               // Crash the peer after specified seconds
	heartbeatIntvl time.Duration     // Heartbeat interval
	heartbeatTimer *time.Timer       // Timer for sending heartbeat
	timeout        time.Duration     // Timeout for detecting missed heartbeats
	crashedPeers   map[int]bool      // Track peers that have crashed or exited
}

// Initialize the peer by reading the configuration file
func NewPeer(configFile string, hostname string, heartbeatIntvl, timeout time.Duration, crashAfter int) (*Peer, error) {
	// Open the configuration file (hostsfile.txt)
	file, err := os.Open(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %v", err)
	}
	defer file.Close()

	// Parse the file to determine the leader and peer IDs
	scanner := bufio.NewScanner(file)
	peerHostnames := make(map[int]string)
	idCounter := 1
	for scanner.Scan() {
		peerHostnames[idCounter] = scanner.Text()
		idCounter++
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	// Resolve hostnames to IP addresses
	hostIPs := make(map[int]string)
	for id, h := range peerHostnames {
		addrs, err := net.LookupHost(h)
		if err != nil || len(addrs) == 0 {
			return nil, fmt.Errorf("failed to resolve hostname %s: %v", h, err)
		}
		hostIPs[id] = addrs[0] // Use the first IP address
	}

	// Find this peer's ID based on its hostname
	peerId := 0
	for id, peerHostname := range peerHostnames {
		if peerHostname == hostname {
			peerId = id
			break
		}
	}

	// Handle the case where the peer's hostname is not found in the hosts file
	if peerId == 0 {
		return nil, fmt.Errorf("hostname %s not found in config file", hostname)
	}

	// The first peer in the list is the leader
	isLeader := (peerId == 1)
	leaderAddr := peerHostnames[1]

	// Initialize peer with the parsed information
	peer := &Peer{
		id:             peerId,
		viewId:         1,             // View ID starts at 1
		membershipList: []int{peerId}, // Start with only this peer
		isLeader:       isLeader,
		leaderAddr:     leaderAddr,
		hostname:       hostname,
		hostnames:      peerHostnames,           // Map of peer IDs to hostnames
		hostIPs:        hostIPs,                 // Map of peer IDs to IP addresses
		acks:           make(map[int]int),       // Tracks acknowledgments
		operations:     make(map[int]Message),   // Store pending operations
		lastHeartbeat:  make(map[int]time.Time), // Last heartbeat for peers
		crashedPeers:   make(map[int]bool),      // Track crashed/exited peers
		crashAfter:     crashAfter,
		heartbeatIntvl: heartbeatIntvl,
		timeout:        timeout, // Timeout is set via command-line flag
	}

	return peer, nil
}

// Listen for incoming messages (TCP for membership management)
func (p *Peer) listenForMessages() {
	listenAddr := fmt.Sprintf(":%d", 8000)
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Println("Error starting listener:", err)
		return
	}
	p.listener = ln
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go p.handleConnection(conn)
	}
}

// Handle an incoming connection
func (p *Peer) handleConnection(conn net.Conn) {
	defer conn.Close()

	var msg Message
	err := json.NewDecoder(conn).Decode(&msg)
	if err != nil {
		fmt.Println("Error decoding message:", err)
		return
	}

	// Dispatch the message based on its type
	switch msg.Type {
	case JOIN:
		p.handleJoin(msg)
	case REQ:
		p.handleReq(msg)
	case OK:
		p.handleOk(msg)
	case NEWVIEW:
		p.handleNewView(msg)
	}
}

// Handle JOIN message (new peer wants to join)
func (p *Peer) handleJoin(msg Message) {
	if p.isLeader {
		p.sendReqToPeers(msg.PeerId) // Initiate the process to add the new peer
	}
}

// Send REQ message to all peers in the membership list, including the new peer
func (p *Peer) sendReqToPeers(newPeerId int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	req := Message{
		Type:         REQ,
		RequestId:    p.viewId,
		ViewId:       p.viewId,
		PeerId:       newPeerId,
		MembershipOp: "ADD", // Operation type is "ADD"
	}

	// Send REQ to all current peers in the membership list
	for _, peerId := range p.membershipList {
		if peerId != p.id { // Do not send REQ to itself
			peerHostname := p.hostnames[peerId]
			p.sendMessageToPeer(peerHostname, req)
		}
	}

	// Send REQ to the new peer (not in the membership list yet)
	newPeerHostname := p.hostnames[newPeerId]
	p.sendMessageToPeer(newPeerHostname, req)

	// Save the operation so it can be handled when all OKs are received
	p.operations[p.viewId] = req
}

// Handle REQ message
func (p *Peer) handleReq(msg Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Save the operation so that it can be performed later
	p.operations[msg.RequestId] = msg

	// Prepare and send the OK message
	ok := Message{
		Type:      OK,
		RequestId: msg.RequestId,
		ViewId:    msg.ViewId,
	}

	// Send OK response back to the leader
	p.sendMessageToPeer(p.leaderAddr, ok)
}

// Handle OK message
func (p *Peer) handleOk(msg Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Count OKs for the request
	p.acks[msg.RequestId]++

	// If OKs from all peers have been received, the leader updates the view
	if p.acks[msg.RequestId] == len(p.membershipList) {
		// Perform the operation: add the new peer
		op := p.operations[msg.RequestId]
		p.membershipList = append(p.membershipList, op.PeerId)

		// Update hostnames and hostIPs with the new peer
		newPeerId := op.PeerId
		newPeerHostname := p.hostnames[newPeerId]
		addrs, err := net.LookupHost(newPeerHostname)
		if err != nil || len(addrs) == 0 {
			fmt.Printf("Error resolving hostname for new peer %d: %v\n", newPeerId, err)
			return
		}
		p.hostIPs[newPeerId] = addrs[0]

		// Increment the view ID
		p.viewId++

		// Broadcast the new view to all peers
		p.broadcastNewView()
	}
}

// Broadcast NEWVIEW message to all peers
func (p *Peer) broadcastNewView() {
	newViewMsg := Message{
		Type:                  NEWVIEW,
		ViewId:                p.viewId,
		UpdatedMembershipList: p.membershipList, // Send the updated membership list
		UpdatedHostnames:      p.hostnames,      // Send updated hostnames
	}

	// Broadcast to all peers in the membership list
	for _, peerId := range p.membershipList {
		peerHostname := p.hostnames[peerId]
		p.sendMessageToPeer(peerHostname, newViewMsg)
	}
}

// Handle NEWVIEW message
func (p *Peer) handleNewView(msg Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Update the peer's view ID and membership list
	p.viewId = msg.ViewId
	p.membershipList = msg.UpdatedMembershipList

	// Update hostnames and hostIPs
	for peerId, hostname := range msg.UpdatedHostnames {
		p.hostnames[peerId] = hostname
		addrs, err := net.LookupHost(hostname)
		if err != nil || len(addrs) == 0 {
			fmt.Printf("Error resolving hostname for peer %d: %v\n", peerId, err)
			continue
		}
		p.hostIPs[peerId] = addrs[0]
	}

	// Initialize or update lastHeartbeat for all peers
	for _, peerId := range p.membershipList {
		if peerId != p.id {
			p.lastHeartbeat[peerId] = time.Now()
		}
	}

	// Print the updated membership view in the required format
	p.printMembershipView()
}

// Print the current membership view
func (p *Peer) printMembershipView() {
	leaderId := 1 // Assume leader ID is always 1
	membership := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(p.membershipList)), ","), "[]")
	fmt.Printf("{peer_id: %d, view_id: %d, leader: %d, memb_list: [%s]}\n",
		p.id, p.viewId, leaderId, membership)
}

// Broadcast HEARTBEAT to all peers using UDP (separate port for heartbeat detection)
func (p *Peer) broadcastHeartbeat() {
	for _, peerId := range p.membershipList {
		if peerId != p.id && !p.crashedPeers[peerId] { // Only send heartbeat to non-crashed peers
			peerIP, exists := p.hostIPs[peerId]
			if !exists {
				continue
			}
			addr := fmt.Sprintf("%s:%d", peerIP, 9000) // Use IP address
			udpAddr, err := net.ResolveUDPAddr("udp", addr)
			if err != nil {
				continue
			}

			msg := Message{Type: HEARTBEAT, PeerId: p.id}
			data, _ := json.Marshal(msg)
			_, err = p.udpConn.WriteToUDP(data, udpAddr)
			if err != nil {
				// Handle error if needed
			}
		}
	}
}

// Listen for UDP HEARTBEAT messages (on port 9000)
func (p *Peer) listenForHeartbeats() {
	buffer := make([]byte, 1024)
	for {
		n, _, err := p.udpConn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}

		var msg Message
		if err := json.Unmarshal(buffer[:n], &msg); err != nil {
			continue
		}

		if msg.Type == HEARTBEAT {
			p.mutex.Lock()
			p.lastHeartbeat[msg.PeerId] = time.Now()
			p.mutex.Unlock()
		}
	}
}

// Start the failure detection mechanism
func (p *Peer) startFailureDetector() {
	ticker := time.NewTicker(p.timeout / 2) // Check failure every half of the timeout
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.detectFailures()
		}
	}
}

// Detect failures by checking the last heartbeat received
func (p *Peer) detectFailures() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	now := time.Now()
	for _, peerId := range p.membershipList {
		if peerId == p.id {
			continue
		}

		lastHeartbeat, ok := p.lastHeartbeat[peerId]
		if !ok || now.Sub(lastHeartbeat) > p.timeout {
			// Peer is considered unreachable, only if it has not crashed already
			if !p.crashedPeers[peerId] {
				leaderMessage := ""
				if peerId == 1 {
					leaderMessage = "(leader)"
				}
				fmt.Printf("{peer_id: %d, view_id: %d, leader: 1, message: \"peer %d %s unreachable\"}\n", p.id, p.viewId, peerId, leaderMessage)

				// Mark peer as crashed so we don't repeatedly report it
				p.crashedPeers[peerId] = true
			}
		}
	}
}

// Print the crash message and exit the program
func (p *Peer) crash() {
	fmt.Printf("{peer_id: %d, view_id: %d, leader: 1, message: \"crashing\"}\n", p.id, p.viewId)

	// Mark this peer as crashed so that it will be detected by others
	p.mutex.Lock()
	p.crashedPeers[p.id] = true
	p.mutex.Unlock()

	os.Exit(0)
}

// Send a message to a specific peer using its hostname with retry mechanism (TCP for membership)
func (p *Peer) sendMessageToPeer(peerHostname string, msg Message) {
	addr := fmt.Sprintf("%s:%d", peerHostname, 8000)
	for retries := 0; retries < 5; retries++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			json.NewEncoder(conn).Encode(&msg)
			conn.Close()
			return
		}
		time.Sleep(1 * time.Second) // Retry after 1 second
	}
	// Optional: Handle the case where the peer cannot be reached
}

func main() {
	// Define command-line flags
	hostFile := flag.String("h", "hostsfile.txt", "Path to the hosts file")
	delay := flag.Int("d", 0, "Delay in seconds for processing JOIN")
	crashAfter := flag.Int("c", 0, "Crash after specified seconds (0 means no crash)")
	heartbeatIntvl := flag.Duration("t", 5*time.Second, "Heartbeat interval duration")   // Reduced heartbeat interval
	timeout := flag.Duration("timeout", 10*time.Second, "Timeout for failure detection") // Timeout is twice the heartbeat interval

	// Parse the flags
	flag.Parse()

	// Get the hostname (set by Docker Compose)
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Error getting hostname:", err)
		return
	}

	// Initialize the peer using the hosts file and hostname
	peer, err := NewPeer(*hostFile, hostname, *heartbeatIntvl, *timeout, *crashAfter)
	if err != nil {
		fmt.Println("Error initializing peer:", err)
		return
	}

	// Create a UDP connection for heartbeats (on port 9000)
	udpAddr, err := net.ResolveUDPAddr("udp", ":9000")
	if err != nil {
		fmt.Println("Error resolving UDP address:", err)
		return
	}

	peer.udpConn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Println("Error creating UDP listener:", err)
		return
	}
	defer peer.udpConn.Close()

	// Start the peer and listen for heartbeats
	go peer.listenForHeartbeats()

	// Start failure detector
	go peer.startFailureDetector()

	// Apply delay specified in CMD parameters
	if *delay > 0 {
		time.Sleep(time.Duration(*delay) * time.Second)
	}

	// Print initial view and membership when peer starts (including leader)
	peer.printMembershipView()

	// If not the leader, send a JOIN message to the leader
	if !peer.isLeader {
		joinMsg := Message{
			Type:   JOIN,
			PeerId: peer.id,
		}
		peer.sendMessageToPeer(peer.leaderAddr, joinMsg) // Send JOIN to the leader
	}

	// Schedule peer crash if the -c flag is provided
	if *crashAfter > 0 {
		go func() {
			time.Sleep(time.Duration(*crashAfter) * time.Second)
			peer.crash()
		}()
	}

	// Send periodic heartbeats (on port 9000)
	peer.heartbeatTimer = time.NewTimer(peer.heartbeatIntvl)
	go func() {
		for {
			<-peer.heartbeatTimer.C
			peer.broadcastHeartbeat()
			peer.heartbeatTimer.Reset(peer.heartbeatIntvl)
		}
	}()

	// Start TCP server for membership messages
	go peer.listenForMessages()

	// Keep the peer running
	select {}
}
