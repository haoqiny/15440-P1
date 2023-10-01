// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"strconv"

	lspnet "github.com/cmu440/lspnet"
)

type server struct {
	receiveChan chan *receivedMessage
	sendChan    chan *Message
	readChan    chan *Message
	closeChan   chan int
	listen      *lspnet.UDPConn
	clientID    int              // used to assign connID for the next connected client
	ClientMap   map[int]*Nclient // a map that uses ConnID as the key and *Nclient as value
}

// struct for receiveChan
type receivedMessage struct {
	addr    *lspnet.UDPAddr // the addr of client
	payload *Message        // the actual msg sent by client
}

// client information used by the server
type Nclient struct {
	ConnID  int              // the connection id given by the server
	addr    *lspnet.UDPAddr  // the addr of client
	Conn    *lspnet.UDPConn  // the UDPConn of client
	seqNum  int              // the server-side sequence number for this particular client
	mapHead int              // the next expected sequence number from client
	msgMap  map[int]*Message // databuffer to store data received from client
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	port_num := strconv.Itoa(port)
	ln, err := lspnet.ResolveUDPAddr("udp", "localhost:"+port_num)
	if err != nil {
		return nil, err
	}
	listen, err := lspnet.ListenUDP("udp", ln)
	if err != nil {
		return nil, err
	}

	svr := &server{
		sendChan:    make(chan *Message),
		readChan:    make(chan *Message),
		receiveChan: make(chan *receivedMessage),
		ClientMap:   make(map[int]*Nclient),
		clientID:    1,
		listen:      listen,
	}

	go readRoutineServer(svr)
	go mainRoutineServer(svr)
	return svr, nil
}

// read an arbitary (but in order) data received by the server
func (s *server) Read() (int, []byte, error) {
	msg := <-s.readChan
	return msg.ConnID, msg.Payload, nil
}

// write to the specified connection ID
func (s *server) Write(connId int, payload []byte) error {
	// prepare new data for sending
	checksum := CalculateChecksum(connId, s.ClientMap[connId].seqNum, len(payload), payload)
	sendMsg := NewData(connId, s.ClientMap[connId].seqNum, len(payload), payload, checksum)

	data, err := json.Marshal(sendMsg)
	if err == nil {
		s.listen.WriteToUDP(data, s.ClientMap[sendMsg.ConnID].addr)
		s.ClientMap[sendMsg.ConnID].seqNum += 1
	}

	return nil
}

// close the connection of a particular client
func (s *server) CloseConn(connId int) error {
	s.ClientMap[connId] = nil
	return nil
}

// close the server
func (s *server) Close() error {
	s.closeChan <- 1
	return nil
}

// handles all incoming or outgoing request for the server
func mainRoutineServer(svr *server) {
	for {
		select {
		case sendMsg := <-svr.sendChan: // if we are trying to send
			data, err := json.Marshal(sendMsg)
			if err == nil {
				svr.listen.WriteToUDP(data, svr.ClientMap[sendMsg.ConnID].addr)
				svr.ClientMap[sendMsg.ConnID].seqNum += 1
			}

		case Received := <-svr.receiveChan: // if we have received msg from client
			handleRequestServer(svr, Received.payload, Received.addr)

		case <-svr.closeChan:
			svr.listen.Close()
		}
	}
}

// when server reads from the UDPConn
func readRoutineServer(svr *server) {
	for {
		select {
		case <-svr.closeChan:
			return
		default:
			payload := make([]byte, 2000)
			n, addr, err := svr.listen.ReadFromUDP(payload)
			if err != nil {
				continue
			}

			var msg Message
			err = json.Unmarshal(payload[:n], &msg)
			if err != nil {
				return
			}

			// put what's read into the receiveChan
			res := &receivedMessage{addr: addr, payload: &msg}
			svr.receiveChan <- res
		}
	}
}

// helper function called in mainRoutine to handle specific incoming msg packet
func handleRequestServer(svr *server, msg *Message, addr *lspnet.UDPAddr) {
	switch msg.Type {
	case MsgConnect: // if msg is connect

		// ack right away
		ackMsg := NewAck(svr.clientID, msg.SeqNum)
		ackData, err := json.Marshal(ackMsg)
		if err == nil {
			svr.listen.WriteToUDP(ackData, addr)
		}

		client := &Nclient{
			ConnID:  svr.clientID,
			addr:    addr,
			seqNum:  msg.SeqNum + 1,
			mapHead: msg.SeqNum + 1,
			msgMap:  make(map[int]*Message),
		}

		// add new client into map
		svr.ClientMap[svr.clientID] = client
		svr.clientID += 1

	case MsgData: // if msg is data
		if len(msg.Payload) < msg.Size {
			return
		}

		// ack right away
		ackMsg := NewAck(msg.ConnID, msg.SeqNum)
		data, err := json.Marshal(ackMsg)
		if err == nil {
			svr.listen.WriteToUDP(data, svr.ClientMap[ackMsg.ConnID].addr)
		}

		svr.ClientMap[msg.ConnID].msgMap[msg.SeqNum] = msg
		for { // if it correspond to the exepcted SN, put the data into readChan
			tempMsg, ok := svr.ClientMap[msg.ConnID].msgMap[svr.ClientMap[msg.ConnID].mapHead]
			if ok {
				svr.readChan <- tempMsg
				svr.ClientMap[msg.ConnID].mapHead += 1
			} else {
				break
			}
		}
	case MsgAck: // if msg is Ack
		return
	case MsgCAck: // if msg is CAck
		return
	}

}
