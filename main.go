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
	JOIN    = "JOIN"
	REQ     = "REQ"
	OK      = "OK"
	NEWVIEW = "NEWVIEW"
)

// Message structure
type Message struct {
	Type                  string
	RequestId             int
	ViewId                int
	PeerId                int
	MembershipOp          string
	UpdatedMembershipList []int // New field for NEWVIEW message
}

// Peer struct to represent each peer
type Peer struct {
	id             int
	viewId         int
	membershipList []int
	isLeader       bool
	leaderAddr     string
	hostname       string
	hostnames      []string // The list of hostnames parsed from the hostsfile
	mutex          sync.Mutex
	listener       net.Listener
	acks           map[int]int     // Tracks acknowledgments for each request ID
	operations     map[int]Message // Stores the pending operations
}

// Initialize the peer by reading the configuration file
func NewPeer(configFile string, hostname string) (*Peer, error) {
	// Open the configuration file (hostsfile.txt)
	file, err := os.Open(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %v", err)
	}
	defer file.Close()

	// Parse the file to determine the leader and peer IDs
	scanner := bufio.NewScanner(file)
	var peerHostnames []string
	for scanner.Scan() {
		peerHostnames = append(peerHostnames, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	// Find this peer's ID based on its hostname
	peerId := 0
	for i, peerHostname := range peerHostnames {
		if peerHostname == hostname {
			peerId = i + 1 // Peer IDs start from 1
			break
		}
	}

	// Handle the case where the peer's hostname is not found in the hosts file
	if peerId == 0 {
		return nil, fmt.Errorf("hostname %s not found in config file", hostname)
	}

	// The first peer in the list is the leader
	isLeader := (peerId == 1)
	leaderAddr := peerHostnames[0]

	// Initialize peer with the parsed information
	peer := &Peer{
		id:             peerId,
		viewId:         1,             // View ID starts at 1
		membershipList: []int{peerId}, // Start with only this peer
		isLeader:       isLeader,
		leaderAddr:     leaderAddr,
		hostname:       hostname,
		hostnames:      peerHostnames, // Store all hostnames from the file
		acks:           make(map[int]int),
		operations:     make(map[int]Message), // Store pending operations
	}

	return peer, nil
}

// Listen for incoming messages
func (p *Peer) listenForMessages() {
	listenAddr := fmt.Sprintf("%s:8000", p.hostname)
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
			peerHostname := p.hostnames[peerId-1] // Use hostname from the parsed list
			p.sendMessageToPeer(peerHostname, req)
		}
	}

	// Send REQ to the new peer (not in the membership list yet)
	newPeerHostname := p.hostnames[newPeerId-1] // Get hostname of new peer
	p.sendMessageToPeer(newPeerHostname, req)

	// Save the operation so it can be handled when all OKs are received
	p.operations[p.viewId] = req
}

// Handle REQ message
func (p *Peer) handleReq(msg Message) {
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
	}

	// Broadcast to all peers in the membership list
	for _, peerId := range p.membershipList {
		peerHostname := p.hostnames[peerId-1] // Use hostname from the parsed list
		p.sendMessageToPeer(peerHostname, newViewMsg)
	}
}

// Handle NEWVIEW message
func (p *Peer) handleNewView(msg Message) {
	// Update the peer's view ID and membership list
	p.viewId = msg.ViewId
	p.membershipList = msg.UpdatedMembershipList

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

// Send a message to a specific peer using its hostname
func (p *Peer) sendMessageToPeer(peerHostname string, msg Message) {
	addr := fmt.Sprintf("%s:8000", peerHostname)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Error connecting to peer %s: %v\n", peerHostname, err)
		return
	}
	defer conn.Close()

	err = json.NewEncoder(conn).Encode(&msg)
	if err != nil {
		fmt.Printf("Error sending message to peer %s: %v\n", peerHostname, err)
	}
}

func main() {
	// Define command-line flags
	hostFile := flag.String("h", "hostsfile.txt", "Path to the hosts file")
	delay := flag.Int("d", 0, "Delay in seconds for processing JOIN")

	// Parse the flags
	flag.Parse()

	// Get the hostname (set by Docker Compose)
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Error getting hostname:", err)
		return
	}

	// Initialize the peer using the hosts file and hostname
	peer, err := NewPeer(*hostFile, hostname)
	if err != nil {
		fmt.Println("Error initializing peer:", err)
		return
	}

	// Start the peer and listen for messages
	go peer.listenForMessages()

	// Print the initial membership view for the leader
	if peer.isLeader {
		peer.printMembershipView()
	}

	// Apply delay specified in CMD parameters
	if *delay > 0 {
		time.Sleep(time.Duration(*delay) * time.Second)
	}

	// If not the leader, send a JOIN message to the leader
	if !peer.isLeader {
		joinMsg := Message{
			Type:   JOIN,
			PeerId: peer.id,
		}
		// Use the leader's hostname dynamically from the peer object
		peer.sendMessageToPeer(peer.leaderAddr, joinMsg) // Send JOIN to the leader
	}

	// Keep the peer running
	select {}
}
