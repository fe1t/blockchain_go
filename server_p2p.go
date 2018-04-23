package main

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/memberlist"
	"github.com/pborman/uuid"
)

const (
	protocol      = "tcp"
	nodeVersion   = 1
	commandLength = 12
)

// Node field
var (
	nodeID   = os.Getenv("NODE_ID")
	nodeSeed = "192.168.0.179"
	// nodeSeed      = "192.168.0.179"
	nodeAddress   string
	miningAddress string
	knownNodes    = []string{fmt.Sprintf("%s:3000", nodeSeed)}
	// knownNodes      = []string{"localhost:3000"}
	blocksInTransit = [][]byte{}
	mempool         = make(map[string]Transaction)
	mybc            = &Blockchain{}
)

// Broadcast field
var (
	UDPBufferSize = 4000
	mtx           sync.RWMutex
	port          = os.Getenv("NODE_ID")
	items         = map[string]string{}
	broadcasts    *memberlist.TransmitLimitedQueue
	thisNode      *memberlist.Memberlist
)

type addr struct {
	AddrList []string
}

type block struct {
	AddrFrom string
	Block    []byte
}

type getblocks struct {
	AddrFrom string
}

type getdata struct {
	AddrFrom string
	Type     string
	ID       []byte
}

type inv struct {
	AddrFrom string
	Type     string
	Items    [][]byte
}

type tx struct {
	AddFrom     string
	Transaction []byte
}

type verzion struct {
	Version    int
	BestHeight int
	AddrFrom   string
}
type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

type delegate struct{}

type update struct {
	Action string // add, del
	Data   map[string]string
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}
	fmt.Println("NotifyMsg")
	spew.Dump(b)
	go handleConnection(b)
	// go handleConnection(b)
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return broadcasts.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte {
	fmt.Println("push pulllll")
	// sendVersion("all", mybc)
	// mtx.RLock()
	// m := items
	// mtx.RUnlock()
	// b, _ := json.Marshal(m)
	return []byte{}
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	// 	if len(buf) == 0 {
	// 		return
	// 	}
	// 	if !join {
	// 		return
	// 	}
	// 	var m map[string]string
	// 	if err := json.Unmarshal(buf, &m); err != nil {
	// 		return
	// 	}
	// 	mtx.Lock()
	// 	for k, v := range m {
	// 		items[k] = v
	// 	}
	// 	mtx.Unlock()
}

func configServer(minerAddress string) error {
	fmt.Println("NODE:", nodeID)
	nodeAddress = fmt.Sprintf("%s:%s", nodeSeed, nodeID)
	miningAddress = minerAddress

	hostname, _ := os.Hostname()
	c := memberlist.DefaultLocalConfig()
	// c.LogOutput = nil
	c.BindAddr = nodeSeed
	if _, err := fmt.Sscanf(port, "%d", &c.BindPort); err != nil {
		log.Panic(err)
	}
	c.PushPullInterval = 0
	c.UDPBufferSize = UDPBufferSize
	c.Delegate = &delegate{}
	c.Name = hostname + "-" + uuid.NewUUID().String()
	m, err := memberlist.Create(c)
	thisNode = m
	if err != nil {
		return err
	}
	if knownNodes != nil {
		_, err := m.Join(knownNodes)
		if err != nil {
			return err
		}
	}
	broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return m.NumMembers()
		},
		RetransmitMult: 1,
	}
	node := m.LocalNode()
	fmt.Printf("Local member %s:%d\n", node.Addr, node.Port)
	return nil
}

func startServer(minerAddress string) {
	if err := configServer(minerAddress); err != nil {
		log.Panic(err)
	}

	mtx.Lock()
	mybc = NewBlockchain(nodeID)
	mtx.Unlock()
	sendVersion("all", mybc)

	select {}

	// for {
	// 	go handleConnection(mybc)
	// }
}

func handleConnection(request []byte) {
	// request, err := ioutil.ReadAll(conn)
	command := bytesToCommand(request[:commandLength])
	fmt.Printf("Received %s command\n", command)

	switch command {
	case "addr":
		handleAddr(request)
	case "block":
		handleBlock(request, mybc)
	case "inv":
		handleInv(request, mybc)
	case "getblocks":
		handleGetBlocks(request, mybc)
	case "getdata":
		handleGetData(request, mybc)
	case "tx":
		handleTx(request, mybc)
	case "version":
		handleVersion(request, mybc)
	default:
		fmt.Println("Unknown command!")
	}
}

func handleAddr(request []byte) {
	var buff bytes.Buffer
	var payload addr

	buff.Write(request[commandLength:])
	dec := gob.NewDecoder(&buff)
	err := dec.Decode(&payload)
	if err != nil {
		log.Panic(err)
	}

	knownNodes = append(knownNodes, payload.AddrList...)
	// TODO: felt: check this later
	fmt.Printf("There are %d known nodes now!\n", len(knownNodes))
	requestBlocks()
}

func handleBlock(request []byte, mybc *Blockchain) {
	var buff bytes.Buffer
	var payload block

	buff.Write(request[commandLength:])
	dec := gob.NewDecoder(&buff)
	err := dec.Decode(&payload)
	if err != nil {
		log.Panic(err)
	}

	blockData := payload.Block
	block := DeserializeBlock(blockData)

	fmt.Println("Recevied a new block!")

	// fe1t: Verify new block before ddding to blockchain
	acceptBlock := true
	if acceptBlock {
		mtx.Lock()
		mybc.AddBlock(block)
		mtx.Unlock()

		fmt.Printf("Added block %x\n", block.Hash)

		if len(blocksInTransit) > 0 {
			blockHash := blocksInTransit[0]
			// TODO: felt: check this later
			sendGetData(payload.AddrFrom, "block", blockHash)

			// TODO: felt: check bug later
			blocksInTransit = blocksInTransit[1:]
		} else {
			mtx.Lock()
			UTXOSet := UTXOSet{mybc}
			mtx.Unlock()
			UTXOSet.Reindex()
		}
	} else {
		fmt.Println("Dropping invalid incoming block...")
	}
}

func handleInv(request []byte, mybc *Blockchain) {
	var buff bytes.Buffer
	var payload inv

	fmt.Println("In handleInv")
	buff.Write(request[commandLength:])
	dec := gob.NewDecoder(&buff)
	err := dec.Decode(&payload)
	if err != nil {
		log.Panic(err)
	}
	spew.Dump(payload)

	fmt.Printf("Received inventory with %d %s\n", len(payload.Items), payload.Type)

	if payload.Type == "block" {
		blocksInTransit = payload.Items

		blockHash := payload.Items[0]
		sendGetData(payload.AddrFrom, "block", blockHash)

		newInTransit := [][]byte{}
		for _, b := range blocksInTransit {
			if bytes.Compare(b, blockHash) != 0 {
				newInTransit = append(newInTransit, b)
			}
		}
		blocksInTransit = newInTransit
	}

	if payload.Type == "tx" {
		txID := payload.Items[0]

		if mempool[hex.EncodeToString(txID)].ID == nil {
			fmt.Println("Getting transaction from", payload.AddrFrom)
			sendGetData(payload.AddrFrom, "tx", txID)
		} else {
			fmt.Println("receive same tx id")
		}
	}
}

func handleGetBlocks(request []byte, mybc *Blockchain) {
	var buff bytes.Buffer
	var payload getblocks

	fmt.Println("In handleGetBlocks")
	buff.Write(request[commandLength:])
	dec := gob.NewDecoder(&buff)
	err := dec.Decode(&payload)
	if err != nil {
		log.Panic(err)
	}
	spew.Dump(payload)

	mtx.RLock()
	blocks := mybc.GetBlockHashes()
	mtx.RUnlock()
	sendInv(payload.AddrFrom, "block", blocks)
}

func handleGetData(request []byte, mybc *Blockchain) {
	var buff bytes.Buffer
	var payload getdata

	fmt.Println("In handleGetData")
	spew.Dump(request)
	buff.Write(request[commandLength:])
	dec := gob.NewDecoder(&buff)
	err := dec.Decode(&payload)
	if err != nil {
		log.Panic(err)
	}
	spew.Dump(payload)

	if payload.Type == "block" {
		mtx.RLock()
		block, err := mybc.GetBlock([]byte(payload.ID))
		mtx.RUnlock()
		if err != nil {
			return
		}

		sendBlock(payload.AddrFrom, &block)
	}

	if payload.Type == "tx" {
		txID := hex.EncodeToString(payload.ID)
		tx := mempool[txID]

		sendTx(payload.AddrFrom, &tx)
		// delete(mempool, txID)
	}
}

func handleTx(request []byte, mybc *Blockchain) {
	var buff bytes.Buffer
	var payload tx

	buff.Write(request[commandLength:])
	dec := gob.NewDecoder(&buff)
	err := dec.Decode(&payload)
	if err != nil {
		log.Panic(err)
	}

	txData := payload.Transaction
	// fmt.Println("In handleTx")
	// spew.Dump(txData)
	tx := DeserializeTransaction(txData)
	// spew.Dump(tx)
	// sTxData := t`x.Serialize()
	// spew.Dump(sTxData)
	// fmt.Println(testSerialization(txData, sTxData)) // true or false ?
	mempool[hex.EncodeToString(tx.ID)] = tx

	// if payload.AddFrom != nodeAddress {
	// 	go sendInv("all", "tx", [][]byte{tx.ID})
	// }

	if len(mempool) >= 2 && len(miningAddress) > 0 {
	MineTransactions:
		var txs []*Transaction
		var usedTXInput [][]byte

		for id := range mempool {
			tx := mempool[id]
			mtx.RLock()
			verified := mybc.VerifyTransaction(&tx)
			mtx.RUnlock()
			if verified {
				if !hasSameTXInput(usedTXInput, tx.Vin) {
					txs = append(txs, &tx)
					for i := range tx.Vin {
						usedTXInput = append(usedTXInput, tx.Vin[i].Txid)
					}
				} else {
					delete(mempool, id)
				}
			}
		}

		if len(txs) == 0 {
			fmt.Println("All transactions are invalid! Waiting for new ones...")
			return
		}

		cbTx := NewCoinbaseTX(miningAddress, "")
		txs = append(txs, cbTx)

		mtx.RLock()
		newBlock := mybc.MineBlock(txs)
		UTXOSet := UTXOSet{mybc}
		mtx.RUnlock()
		UTXOSet.Reindex()

		fmt.Println("New block is mined!")

		for _, tx := range txs {
			txID := hex.EncodeToString(tx.ID)
			delete(mempool, txID)
			// delete(mempool, used)
		}

		for _, node := range knownNodes {
			if node != nodeAddress {
				sendInv(node, "block", [][]byte{newBlock.Hash})
			}
		}

		if len(mempool) > 0 {
			goto MineTransactions
		}
	}
}

func handleVersion(request []byte, mybc *Blockchain) {
	var buff bytes.Buffer
	var payload verzion

	buff.Write(request[commandLength:])
	dec := gob.NewDecoder(&buff)
	err := dec.Decode(&payload)
	if err != nil {
		log.Panic(err)
	}

	mtx.Lock()
	myBestHeight := mybc.GetBestHeight()
	mtx.Unlock()
	foreignerBestHeight := payload.BestHeight

	if myBestHeight < foreignerBestHeight {
		fmt.Println("ask for blocks")
		sendGetBlocks(payload.AddrFrom)
	} else if myBestHeight > foreignerBestHeight {
		fmt.Println("ask for version")
		sendVersion(payload.AddrFrom, mybc)
	}

	// sendAddr(payload.AddrFrom)
	if !nodeIsKnown(payload.AddrFrom) {
		knownNodes = append(knownNodes, payload.AddrFrom)
	}
}

func sendAddr(toAddr string) {
	nodes := addr{knownNodes}
	nodes.AddrList = append(nodes.AddrList, nodeAddress)
	payload := gobEncode(nodes)
	request := append(commandToBytes("addr"), payload...)

	sendData(toAddr, request)
}

func sendBlock(toAddr string, b *Block) {
	data := block{nodeAddress, b.Serialize()}
	payload := gobEncode(data)
	request := append(commandToBytes("block"), payload...)

	sendData(toAddr, request)
}

func sendData(toAddr string, data []byte) {
	fmt.Println("In sendData")
	spew.Dump(toAddr)
	spew.Dump(data)
	if thisNode == nil {
		if err := configServer(""); err != nil {
			log.Panic(err)
		}
	}
	if toAddr == "all" {
		spew.Dump(data)
		broadcasts.QueueBroadcast(&broadcast{
			msg:    data,
			notify: nil,
		})
		select {}
	} else {
		if bytesToCommand(data[:commandLength]) == "getdata" {
			fmt.Println("Finding BUG")
			// trying to parse data
			var buff bytes.Buffer
			var payload getdata

			buff.Write(data[commandLength:])
			dec := gob.NewDecoder(&buff)
			err := dec.Decode(&payload)
			if err != nil {
				log.Panic(err)
			}
			spew.Dump(payload)
			fmt.Println(payload.AddrFrom)
		}
		addrTuple := strings.Split(toAddr, ":")
		spew.Dump(addrTuple)
		ip := addrTuple[0]
		port, err := strconv.ParseUint(addrTuple[1], 10, 16)
		if err != nil {
			log.Panic(err)
		}
		fmt.Println(net.ParseIP(ip))
		fmt.Println(uint16(port))
		node := &memberlist.Node{Addr: net.ParseIP(ip), Port: uint16(port)}
		spew.Dump(node)
		fmt.Println((*node).Addr)
		err = thisNode.SendReliable(node, data)
		if err != nil {
			log.Panic(err)
		}
	}
}

func sendInv(toAddr, kind string, items [][]byte) {
	inventory := inv{nodeAddress, kind, items}
	payload := gobEncode(inventory)
	request := append(commandToBytes("inv"), payload...)

	sendData(toAddr, request)
}

func sendGetBlocks(toAddr string) {
	payload := gobEncode(getblocks{nodeAddress})
	request := append(commandToBytes("getblocks"), payload...)

	sendData(toAddr, request)
}

func sendGetData(toAddr, kind string, id []byte) {
	if nodeAddress == "" {
		nodeAddress = fmt.Sprintf("%s:%s", nodeSeed, os.Getenv("NODE_ID"))
	}
	payload := gobEncode(getdata{nodeAddress, kind, id})
	request := append(commandToBytes("getdata"), payload...)

	sendData(toAddr, request)
}

func sendTx(toAddr string, tnx *Transaction) {
	// fmt.Println("In sendTx")
	// spew.Dump(*tnx)
	// spew.Dump(tnx.Serialize())
	data := tx{nodeAddress, tnx.Serialize()}
	payload := gobEncode(data)
	request := append(commandToBytes("tx"), payload...)

	sendData(toAddr, request)
}

func sendVersion(toAddr string, mybc *Blockchain) {
	mtx.RLock()
	bestHeight := mybc.GetBestHeight()
	mtx.RUnlock()
	payload := gobEncode(verzion{nodeVersion, bestHeight, nodeAddress})
	request := append(commandToBytes("version"), payload...)

	sendData(toAddr, request)
}

func requestBlocks() {
	// sendGetBlocks(node)
}

func commandToBytes(command string) []byte {
	var bytes [commandLength]byte

	for i, c := range command {
		bytes[i] = byte(c)
	}

	return bytes[:]
}

func bytesToCommand(bytes []byte) string {
	var command []byte

	for _, b := range bytes {
		if b == 0x0 {
			break
		}
		command = append(command, b)
	}

	return fmt.Sprintf("%s", command)
}

func gobEncode(data interface{}) []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(data)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

func nodeIsKnown(addr string) bool {
	for _, node := range knownNodes {
		if node == addr {
			return true
		}
	}

	return false
}

func testSerialization(s1, s2 []byte) bool {
	return bytes.Equal(s1, s2)
}

func hasSameTXInput(listByte [][]byte, inputs []TXInput) bool {
	for i := range inputs {
		for _, val := range listByte {
			if bytes.Compare(val, inputs[i].Txid) == 0 {
				return true
			}
		}
	}
	return false
}
