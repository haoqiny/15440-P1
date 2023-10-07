// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"

	lspnet "github.com/cmu440/lspnet"
)

type client struct {
	connId      int              // The connection id given by the server
	receiveChan chan *Message    // intake anything sent to the client by the server
	readChan    chan []byte      // channel for Read()
	writeChan    chan []byte    // channel for msg send to server
	closeChan   chan int         // channel for signalling close
	conn        *lspnet.UDPConn  // UDP connection
	SeqNum      int              // client side SN
	mapHead     int              // the SN we are looking for next
	SWmin       int              // the min index of the sliding window
	MsgMap      map[int]*Message // map for storing all (possible out-of-order) incoming data
	UnackedMap  map[int]*Message // map for storing unacked messages
	WriteMap    map[int]*Message
	params      *Params
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// initialSeqNum is an int representing the Initial Sequence Number (ISN) this
// client must use. You may assume that sequence numbers do not wrap around.
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, initialSeqNum int, params *Params) (Client, error) {
	
	udpAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	udpConn, err := lspnet.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	// send a connect request to the server
	connMsg := NewConnect(initialSeqNum)
	data, err := json.Marshal(connMsg)
	if err == nil {
		udpConn.Write(data)

	}

	// read for connection acknowledgement
	ackData := make([]byte, 2000)
	n, ackErr := udpConn.Read(ackData)
	if ackErr != nil {
		return nil, ackErr
	}

	var msg Message
	err = json.Unmarshal(ackData[:n], &msg)

	if err != nil {
		return nil, err
	}

	cli := &client{
		connId:      msg.ConnID, // use the returned connectionID to construct new client instance
		receiveChan: make(chan *Message),
		writeChan:    make(chan []byte),
		readChan:    make(chan []byte),
		closeChan:   make(chan int),
		conn:        udpConn,
		SeqNum:      initialSeqNum + 1,
		mapHead:     initialSeqNum + 1,
		SWmin:       initialSeqNum + 1,
		MsgMap:      make(map[int]*Message),
		UnackedMap:  make(map[int]*Message),
		WriteMap:    make(map[int]*Message),
		params:      params,
	}

	go readRoutine(cli)
	go mainRoutine(cli)

	return cli, nil
}

// return the connection ID assignment by the server
func (c *client) ConnID() int {
	return c.connId
}

// Read() takes the elem from readChan directory (possibly wait indefinitely)
func (c *client) Read() ([]byte, error) {
	data := <-c.readChan
	return data, nil
}

// Write() generates the new data msg and put into sendChan
func (c *client) Write(payload []byte) error {
	
	c.writeChan <- payload

	return nil
}

// close client
func (c *client) Close() error {
	c.closeChan <- 1
	return nil
}

// read from client.conn and put the packet into receiveChan
func readRoutine(cli *client) {
	for {
		payload := make([]byte, 2000)
		n, err := cli.conn.Read(payload)
		if err != nil {
			return
		}

		var msg Message
		err = json.Unmarshal(payload[:n], &msg)
		if err != nil {
			return
		}
		cli.receiveChan <- &msg
	}
}

// routine that handles all incoming or outgoing requests for client
func mainRoutine(cli *client) {
	for {
		select {
		case msg := <-cli.receiveChan: // if we received anything from server
			handleRead(cli, msg)

		case payload := <-cli.writeChan: // if we want to send anything to server
			checksum := CalculateChecksum(cli.connId, cli.SeqNum, len(payload), payload)
			sendMsg := NewData(cli.connId, cli.SeqNum, len(payload), payload, checksum)
			//fmt.Println(sendMsg.SeqNum)
			cli.WriteMap[sendMsg.SeqNum] = sendMsg
			cli.SeqNum ++
			handleWrite(cli)

		case <-cli.closeChan:
			cli.conn.Close()
			return
		}
	}
}

func handleWrite(cli *client) {
	
	for {
		_, ok1 := cli.UnackedMap[cli.SWmin]
		_, ok2 := cli.WriteMap[cli.SWmin]
		if !ok1 && !ok2 && cli.SWmin < cli.SeqNum { //has been acked
			cli.SWmin += 1
		} else {
			break
		}
	}
	SWmax := cli.SWmin + cli.params.WindowSize - 1
	for i := cli.SWmin; i <= SWmax; i++{
		sendMsg, ok := cli.WriteMap[i]
		//fmt.Println(cli.SeqNum)
		//fmt.Println(cli.SWmin)
		
		if ok && len(cli.UnackedMap) < cli.params.MaxUnackedMessages{
			data, _ := json.Marshal(sendMsg)
			//fmt.Println(sendMsg)
			cli.conn.Write(data)
			cli.UnackedMap[sendMsg.SeqNum] = sendMsg
			delete(cli.WriteMap,sendMsg.SeqNum)
		} 
	}
	
	

}
// helper function called in mainRoutine to handle specific incoming msg packet
func handleRead(cli *client, msg *Message) {
	switch msg.Type {
	case MsgData: // if msg is data
		ackMsg := NewAck(cli.connId, msg.SeqNum)
		data, err := json.Marshal(ackMsg)
		if err == nil {
			cli.conn.Write(data)
		}

		cli.MsgMap[msg.SeqNum] = msg
		for {
			tempMsg, ok := cli.MsgMap[cli.mapHead]
			if ok {
				cli.readChan <- tempMsg.Payload
				cli.mapHead += 1
			} else {
				break
			}
		}

	case MsgAck: // if msg is Ack
		ackSeqNum := msg.SeqNum
		if cli.UnackedMap[ackSeqNum] == nil {
			//fmt.Println("error occured, acking a nil message")
			return
		}
		delete(cli.UnackedMap, ackSeqNum)
		handleWrite(cli)
		return

	case MsgCAck: // if msg is CAck
		cackSeqNum := msg.SeqNum
		for i := 1; i <= cackSeqNum; i++ {
			if cli.UnackedMap[i] != nil {
				delete(cli.UnackedMap, i)
			}
		}
		handleWrite(cli)
		return
	}
}