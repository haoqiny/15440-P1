// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	lspnet "github.com/cmu440/lspnet"
)

type client struct {
	connId      int              // The connection id given by the server
	receiveChan chan *Message    // intake anything sent to the client by the server
	readChan    chan []byte      // channel for Read()
	sendChan    chan *Message    // channel for msg send to server
	closeChan   chan int         // channel for signalling close
	conn        *lspnet.UDPConn  // UDP connection
	SeqNum      int              // client side SN
	mapHead     int              // the SN we are looking for next
	SWmin       int              // the min index of the sliding window
	MsgMap      map[int]*Message // map for storing all (possible out-of-order) incoming data
	UnackedMap  map[int]*cliMsg // map for storing unacked messages
	params      *Params
	ticker		*time.Ticker
	epoch		int
	CurrentBackoff int
}

type cliMsg struct {
	Message		*Message
	BackoffIndex int
	CurrentBackoff int
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
	ticker := time.NewTicker(2*time.Second)
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

	timer := time.NewTimer(time.Duration(params.EpochMillis) * time.Millisecond)

	// read for connection acknowledgement
	ackData := make([]byte, 2000)
	n, ackErr := udpConn.Read(ackData)

	if ackErr != nil {
		currEpoch := 0
		stay := true
		for stay {
			if currEpoch == params.EpochLimit {
				return nil, errors.New("reach epoch limit, connection dead")
			}
			select {
			case <-timer.C:
				currEpoch += 1
				udpConn.Write(data)
				_, ackErr = udpConn.Read(ackData)
				if ackErr == nil {
					stay = false
				}
			}
		}
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
		sendChan:    make(chan *Message),
		readChan:    make(chan []byte),
		closeChan:   make(chan int),
		conn:        udpConn,
		SeqNum:      initialSeqNum + 1,
		mapHead:     initialSeqNum + 1,
		SWmin:       initialSeqNum + 1,
		MsgMap:      make(map[int]*Message),
		UnackedMap:  make(map[int]*cliMsg),
		params:      params,
		ticker: 	 ticker,
		epoch:		 0, 
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
	SWmax := c.SWmin + c.params.WindowSize - 1
	if c.SeqNum < c.SWmin || c.SeqNum > SWmax { //seqNum not in window
		fmt.Printf("SeqNum: %d, SWmin: %d, seqNum not in window!\n", c.SeqNum, c.SWmin)
		return nil
	}
	if len(c.UnackedMap) >= c.params.MaxUnackedMessages {
		fmt.Printf("MaxUnackedMsg: %d, currUnackedMsg: %d, cant send!", c.params.MaxUnackedMessages, len(c.UnackedMap))
		return nil
	}
	// prepare send data
	checksum := CalculateChecksum(c.connId, c.SeqNum, len(payload), payload)
	msg := NewData(c.connId, c.SeqNum, len(payload), payload, checksum)

	c.sendChan <- msg
	
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
		case <-cli.ticker.C:
			for _, value := range cli.UnackedMap {
				if value.CurrentBackoff == 0{
					data, err := json.Marshal(value.Message)
					if err == nil {
						cli.conn.Write(data)
						nextEpoch := int(math.Exp(float64(value.BackoffIndex)))
						if nextEpoch > cli.params.MaxBackOffInterval {
							nextEpoch = nextEpoch / 2
						}
						value.CurrentBackoff = nextEpoch
						value.BackoffIndex += 1
					}
				} else {
					value.CurrentBackoff -= 1
				}
			}
			cli.epoch++
		case msg := <-cli.receiveChan: // if we received anything from server
			handleRequest(cli, msg)

		case sendMsg := <-cli.sendChan: // if we want to send anything to server
			data, err := json.Marshal(sendMsg)
			if err == nil {
				res := &cliMsg{CurrentBackoff: 0, BackoffIndex: 0, Message: sendMsg}
				cli.conn.Write(data)
				cli.UnackedMap[cli.SeqNum] = res
				cli.SeqNum += 1
			}
		case <-cli.closeChan:
			cli.conn.Close()
			return
		}
	}
}

// helper function called in mainRoutine to handle specific incoming msg packet
func handleRequest(cli *client, msg *Message) {
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
			fmt.Println("error occured, acking a nil message")
			return
		}
		delete(cli.UnackedMap, ackSeqNum)
		for {
			_, ok := cli.UnackedMap[cli.SWmin]
			if !ok && cli.SWmin < cli.SeqNum { //has been acked
				//fmt.Printf("SWmin: %d -> %d\n", cli.SWmin, cli.SWmin+1)
				cli.SWmin += 1
			} else {
				break
			}
		}
		return

	case MsgCAck: // if msg is CAck
		cackSeqNum := msg.SeqNum
		for i := 1; i <= cackSeqNum; i++ {
			if cli.UnackedMap[i] != nil {
				delete(cli.UnackedMap, i)
			}
		}
		for {
			_, ok := cli.UnackedMap[cli.SWmin]
			if !ok && cli.SWmin < cli.SeqNum { //has been acked
				cli.SWmin += 1
			} else {
				break
			}
		}
		return
	}
}
