// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	lspnet "github.com/cmu440/lspnet"
)

type server struct {
	receiveChan     chan *receivedMessage
	writeChan       chan *writeMessage //msg with epoch
	readChan        chan *Message
	readRequest     chan int
	closeClientChan chan int
	closeChan       chan int
	listen          *lspnet.UDPConn
	clientID        int              // used to assign connID for the next connected client
	ClientMap       map[int]*Nclient // a map that uses ConnID as the key and *Nclient as value
	params          *Params
	pendingRead     bool
	ticker          *time.Ticker
	epoch           int
}

// struct for receiveChan
type receivedMessage struct {
	addr    *lspnet.UDPAddr // the addr of client
	payload *Message        // the actual msg sent by client
}

type writeMessage struct {
	connId  int
	payload []byte
}

// client information used by the server
type Nclient struct {
	connID     int              // the connection id given by the server
	addr       *lspnet.UDPAddr  // the addr of client
	Conn       *lspnet.UDPConn  // the UDPConn of client
	SeqNum     int              // the server-side sequence number for this particular client
	mapHead    int              // the next expected sequence number from client
	SWmin      int              // min index for sliding window
	msgMap     map[int]*Message // databuffer to store data received from client
	UnackedMap map[int]*cliMsg  //unacked messages
	WriteMap   map[int]*Message
	epoch      int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	ticker := time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond)
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
		writeChan:       make(chan *writeMessage),
		readChan:        make(chan *Message),
		readRequest:     make(chan int),
		receiveChan:     make(chan *receivedMessage),
		ClientMap:       make(map[int]*Nclient),
		clientID:        1,
		listen:          listen,
		params:          params,
		pendingRead:     false,
		ticker:          ticker,
		epoch:           0,
		closeClientChan: make(chan int),
		closeChan:       make(chan int),
	}

	go readRoutineServer(svr)
	go mainRoutineServer(svr)
	return svr, nil
}

// read an arbitary (but in order) data received by the server
func (s *server) Read() (int, []byte, error) {
	//fmt.Println("server calls Read() function")
	s.readRequest <- 1
	//fmt.Println("server sends a 1 into readRequest chan")
	msg := <-s.readChan
	//fmt.Println("server finished reading")
	return msg.ConnID, msg.Payload, nil
}

// write to the specified connection ID
func (s *server) Write(connId int, payload []byte) error {
	data := &writeMessage{
		connId:  connId,
		payload: payload,
	}

	s.writeChan <- data

	return nil
}

// close the connection of a particular client
func (s *server) CloseConn(connId int) error {
	// if s.ClientMap[connId] == nil {
	//  return errors.New("reach epoch limit, connection dead")
	// }
	s.closeClientChan <- connId
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
		case id := <-svr.closeClientChan:
			if svr.ClientMap[id] == nil {
				delete(svr.ClientMap, id)
			}
		case <-svr.readRequest:
			//fmt.Println("server trying to read something")
			read := 0
			for _, client := range svr.ClientMap {
				Msg, ok := client.msgMap[client.mapHead]
				if ok {
					svr.readChan <- Msg
					client.mapHead += 1
					read += 1
					break
				}
			}
			if read == 0 {
				svr.pendingRead = true
			}
		case sendMsg := <-svr.writeChan: // if we are trying to send
			cli := svr.ClientMap[sendMsg.connId]
			checksum := CalculateChecksum(cli.connID, cli.SeqNum, len(sendMsg.payload), sendMsg.payload)
			msg := NewData(cli.connID, cli.SeqNum, len(sendMsg.payload), sendMsg.payload, checksum)
			//fmt.Println(sendMsg.SeqNum)
			cli.WriteMap[msg.SeqNum] = msg
			cli.SeqNum++
			handleWriteServer(svr, cli)

		case Received := <-svr.receiveChan: // if we have received msg from client
			//fmt.Println("server received something")
			handleReadServer(svr, Received.payload, Received.addr)
			//fmt.Println("server read")

		case <-svr.closeChan:
			svr.listen.Close()

		case <-svr.ticker.C:
			// fmt.Println("enter tikers")
			for index, client := range svr.ClientMap {
				// fmt.Println("llllllllllll")
				// fmt.Println(client)
				if client.epoch == svr.params.EpochLimit {
					delete(svr.ClientMap, index)
					continue
				}
				for _, value := range client.UnackedMap {
					if value.CurrentBackoff == 0 {
						data, err := json.Marshal(value.Message)
						if err == nil {
							// fmt.Println(client.Conn)
							svr.listen.WriteToUDP(data, client.addr)

							var nextEpoch int
							if value.PrevBackoff == 0 {
								nextEpoch = 1
							} else {
								nextEpoch = value.PrevBackoff * 2
							}

							if nextEpoch > svr.params.MaxBackOffInterval {
								nextEpoch = nextEpoch / 2
							}
							value.CurrentBackoff = nextEpoch
							value.PrevBackoff = value.CurrentBackoff
						}
					} else {
						value.CurrentBackoff -= 1
					}
				}
				client.epoch++
			}
			svr.epoch++
		}
	}
}

func handleWriteServer(svr *server, cli *Nclient) {
	//fmt.Println("enter handleWriteSvr")
	for {
		_, ok1 := cli.UnackedMap[cli.SWmin]
		_, ok2 := cli.WriteMap[cli.SWmin]
		if !ok1 && !ok2 && cli.SWmin < cli.SeqNum { //has been acked
			cli.SWmin += 1
		} else {
			break
		}
	}
	SWmax := cli.SWmin + svr.params.WindowSize - 1
	for i := cli.SWmin; i <= SWmax; i++ {
		sendMsg, ok := cli.WriteMap[i]
		//fmt.Println(cli.SeqNum)
		//fmt.Println(cli.SWmin)

		if ok && len(cli.UnackedMap) < svr.params.MaxUnackedMessages {
			data, _ := json.Marshal(sendMsg)
			//fmt.Println(sendMsg)
			svr.listen.WriteToUDP(data, cli.addr)
			res := &cliMsg{CurrentBackoff: 0, PrevBackoff: 0, Message: sendMsg}
			cli.UnackedMap[sendMsg.SeqNum] = res
			delete(cli.WriteMap, sendMsg.SeqNum)
		}
	}
	//fmt.Println("exit handleWriteSvr")
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
func handleReadServer(svr *server, msg *Message, addr *lspnet.UDPAddr) {
	switch msg.Type {
	case MsgConnect: // if msg is connect
		fmt.Println("server read a connect")

		// ack right away

		ackMsg := NewAck(svr.clientID, msg.SeqNum)
		ackData, err := json.Marshal(ackMsg)
		if err == nil {
			svr.listen.WriteToUDP(ackData, addr)
			fmt.Println("server write back a connect ack")
		}

		client := &Nclient{
			connID:     svr.clientID,
			addr:       addr,
			SeqNum:     msg.SeqNum + 1,
			mapHead:    msg.SeqNum + 1,
			SWmin:      msg.SeqNum + 1,
			msgMap:     make(map[int]*Message),
			UnackedMap: make(map[int]*cliMsg),
			WriteMap:   make(map[int]*Message),
			epoch:      0,
		}

		// add new client into map
		svr.ClientMap[svr.clientID] = client
		svr.clientID += 1
		//fmt.Println("server finish reading a connect")
	case MsgData: // if msg is data
		//fmt.Println("server read a data")
		if len(msg.Payload) < msg.Size {
			return
		} else if len(msg.Payload) > msg.Size {
			msg.Payload = msg.Payload[:msg.Size]
		}
		if CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload) != msg.Checksum {
			return
		}

		// ack right away
		ackMsg := NewAck(msg.ConnID, msg.SeqNum)
		data, err := json.Marshal(ackMsg)
		if err == nil {
			svr.listen.WriteToUDP(data, svr.ClientMap[ackMsg.ConnID].addr)
		}
		svr.ClientMap[msg.ConnID].msgMap[msg.SeqNum] = msg
		// if it correspond to the exepcted SN, put the data into readChan
		for _, client := range svr.ClientMap {
			tempMsg, ok := client.msgMap[client.mapHead]
			if ok {
				//fmt.Println("send to readchan")
				if svr.pendingRead {
					svr.readChan <- tempMsg
					svr.pendingRead = false
					//fmt.Println("finish send to readchan")
					client.mapHead += 1
				}

			}
		}
		svr.ClientMap[msg.ConnID].epoch = 0

	case MsgAck: // if msg is Ack
		//fmt.Println("server read an Ack")

		cli := svr.ClientMap[msg.ConnID]
		ackSeqNum := msg.SeqNum
		if cli.UnackedMap[ackSeqNum] == nil {
			fmt.Println("error occured, acking a nil message")
			return
		}
		delete(cli.UnackedMap, ackSeqNum)
		handleWriteServer(svr, cli)
		svr.ClientMap[msg.ConnID].epoch = 0
		//fmt.Println("server finished reading Ack")
		return
	case MsgCAck: // if msg is CAck
		//fmt.Println("server read an CAck")

		cli := svr.ClientMap[msg.ConnID]
		cackSeqNum := msg.SeqNum
		for i := 1; i <= cackSeqNum; i++ {
			if cli.UnackedMap[i] != nil {
				delete(cli.UnackedMap, i)
			}
		}
		handleWriteServer(svr, cli)
		svr.ClientMap[msg.ConnID].epoch = 0
		//fmt.Println("server finished reading an CAck")
		return
	}

}
