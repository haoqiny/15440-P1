// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"time"

	lspnet "github.com/cmu440/lspnet"
)

type client struct {
	connId           int           // The connection id given by the server
	receiveChan      chan *Message // intake anything sent to the client by the server
	readChan         chan *Message // channel for Read()
	writeChan        chan []byte   // channel for msg send to server
	closeChan        chan int      // channel for signalling close
	closeReceiveChan chan int
	readRequest      chan int
	conn             *lspnet.UDPConn  // UDP connection
	SeqNum           int              // client side SN
	mapHead          int              // the SN we are looking for next
	SWmin            int              // the min index of the sliding window
	MsgMap           map[int]*Message // map for storing all (possible out-of-order) incoming data
	UnackedMap       map[int]*cliMsg  // map for storing unacked messages
	WriteMap         map[int]*Message // msgs that have not been transmitted due to sw
	params           *Params
	pendingRead      bool
	ticker           *time.Ticker
	epoch            int
	heartbeat        bool
	closing          bool
}

type cliMsg struct {
	Message        *Message
	PrevBackoff    int
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

	ticker := time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond)
	udpAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	udpConn, err := lspnet.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	connMsg := NewConnect(initialSeqNum)
	data, err := json.Marshal(connMsg)
	if err == nil {
		udpConn.Write(data)
	}

	connectionTicker := time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond)
	// send a connect request to the server

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
			case <-connectionTicker.C:

				currEpoch += 1
				udpConn.Write(data)
				n, ackErr = udpConn.Read(ackData)

				if ackErr == nil {

					stay = false
				}
			}
		}

	}

	var msg Message
	err = json.Unmarshal(ackData[:n], &msg)

	if err != nil {
		return nil, err
	}

	cli := &client{
		connId:           msg.ConnID, // use the returned connectionID to construct new client instance
		receiveChan:      make(chan *Message),
		writeChan:        make(chan []byte),
		readChan:         make(chan *Message),
		closeChan:        make(chan int),
		closeReceiveChan: make(chan int),
		readRequest:      make(chan int),
		conn:             udpConn,
		SeqNum:           initialSeqNum + 1,
		mapHead:          initialSeqNum + 1,
		SWmin:            initialSeqNum + 1,
		MsgMap:           make(map[int]*Message),
		UnackedMap:       make(map[int]*cliMsg),
		WriteMap:         make(map[int]*Message),
		params:           params,
		pendingRead:      false,
		ticker:           ticker,
		epoch:            0,
		heartbeat:        false,
		closing:          false,
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
	// fmt.Println("read() function being called")
	c.readRequest <- 1
	// fmt.Println("read() function put a 1 into c.readRequest")
	//fmt.Println("server sends a 1 into readRequest chan")
	msg := <-c.readChan
	// fmt.Println("Im out!!!")
	//fmt.Println("server finished reading")
	if msg == nil {
		// fmt.Println("read() returned")
		return nil, errors.New("reading from closed client")
	}
	//fmt.Println("read() returned")
	return msg.Payload, nil
}

// Write() generates the new data msg and put into sendChan
func (c *client) Write(payload []byte) error {

	c.writeChan <- payload

	return nil
}

// close client
func (c *client) Close() error {
	c.closeChan <- 1
	<-c.closeReceiveChan
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
		case <-cli.readRequest:
			//fmt.Println("Client close request detected")
			if cli.closing {
				//fmt.Println("Client closing detected")
				cli.readChan <- nil
				continue
			}
			//fmt.Println("server trying to read something")
			read := 0

			Msg, ok := cli.MsgMap[cli.mapHead]
			if ok {
				cli.readChan <- Msg
				cli.mapHead += 1
				read += 1
				break
			}

			if read == 0 {
				cli.pendingRead = true
				//fmt.Println("cli.pendingRead = true")
			}
		case msg := <-cli.receiveChan: // if we received anything from server
			handleRead(cli, msg)
			//fmt.Println("client received the following msg:")
			//fmt.Println(msg)

			if cli.closing && len(cli.WriteMap) == 0 && len(cli.UnackedMap) == 0 {
				cli.conn.Close()
				cli.closeReceiveChan <- 1
				return
			}

		case payload := <-cli.writeChan: // if we want to send anything to server
			checksum := CalculateChecksum(cli.connId, cli.SeqNum, len(payload), payload)
			sendMsg := NewData(cli.connId, cli.SeqNum, len(payload), payload, checksum)
			//fmt.Println(sendMsg.SeqNum)
			if cli.closing {
				continue
			}
			cli.WriteMap[sendMsg.SeqNum] = sendMsg
			cli.SeqNum++
			handleWrite(cli)

		case <-cli.closeChan:
			if len(cli.WriteMap) == 0 && len(cli.UnackedMap) == 0 {
				cli.conn.Close()
				cli.closeReceiveChan <- 1
				return
			} else {
				cli.closing = true
			}

		case <-cli.ticker.C:
			// fmt.Println(cli.epoch)
			if cli.epoch == cli.params.EpochLimit {
				cli.closing = true
				cli.conn.Close()
				if cli.pendingRead {
					cli.readChan <- nil
				}
				return
			}

			if cli.heartbeat { // if need to send heartbeat
				heartbeat := NewAck(cli.connId, 0)
				data, err := json.Marshal(heartbeat)
				if err == nil {
					cli.conn.Write(data)
				}
			}

			cli.heartbeat = true
			for _, value := range cli.UnackedMap {
				if value.CurrentBackoff == 0 {
					data, err := json.Marshal(value.Message)
					if err == nil {
						cli.conn.Write(data)
						if value.Message.Type == MsgData {
							cli.heartbeat = false
						}
						var nextEpoch int
						if value.PrevBackoff == 0 {
							nextEpoch = 1
						} else {
							nextEpoch = value.PrevBackoff * 2
						}

						if nextEpoch > cli.params.MaxBackOffInterval {
							nextEpoch = nextEpoch / 2
						}
						value.CurrentBackoff = nextEpoch
						value.PrevBackoff = value.CurrentBackoff
					}
				} else {
					value.CurrentBackoff -= 1
				}
			}
			cli.epoch++
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
	for i := cli.SWmin; i <= SWmax; i++ {
		sendMsg, ok := cli.WriteMap[i]
		//fmt.Println(cli.SeqNum)
		//fmt.Println(cli.SWmin)

		if ok && len(cli.UnackedMap) < cli.params.MaxUnackedMessages {
			data, _ := json.Marshal(sendMsg)
			//fmt.Println(sendMsg)
			cli.conn.Write(data)
			cli.heartbeat = false
			res := &cliMsg{CurrentBackoff: 0, PrevBackoff: 0, Message: sendMsg}
			cli.UnackedMap[sendMsg.SeqNum] = res
			delete(cli.WriteMap, sendMsg.SeqNum)
		}
	}

}

// helper function called in mainRoutine to handle specific incoming msg packet
func handleRead(cli *client, msg *Message) {
	switch msg.Type {
	case MsgData: // if msg is data
		if len(msg.Payload) < msg.Size {
			return
		} else if len(msg.Payload) > msg.Size {
			msg.Payload = msg.Payload[:msg.Size]
		}

		if CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload) != msg.Checksum {
			return
		}

		ackMsg := NewAck(cli.connId, msg.SeqNum)
		data, err := json.Marshal(ackMsg)
		if err == nil {
			cli.conn.Write(data)
		}

		cli.MsgMap[msg.SeqNum] = msg
		tempMsg, ok := cli.MsgMap[cli.mapHead]
		if ok {
			//fmt.Println("send to readchan")
			if cli.pendingRead {
				cli.readChan <- tempMsg
				cli.pendingRead = false
				//fmt.Println("finish send to readchan")
				cli.mapHead += 1
			}

		}
		cli.epoch = 0

	case MsgAck: // if msg is Ack
		ackSeqNum := msg.SeqNum
		_, ok := cli.UnackedMap[ackSeqNum]
		if !ok {
			//fmt.Println("error occured, acking a nil message")
			return
		}
		delete(cli.UnackedMap, ackSeqNum)
		handleWrite(cli)
		if !(cli.closing && ackSeqNum == 0) {
			cli.epoch = 0
		}
		return

	case MsgCAck: // if msg is CAck
		cackSeqNum := msg.SeqNum
		for i := 1; i <= cackSeqNum; i++ {
			if cli.UnackedMap[i] != nil {
				delete(cli.UnackedMap, i)
			}
		}
		handleWrite(cli)
		cli.epoch = 0
		return
	}
}
