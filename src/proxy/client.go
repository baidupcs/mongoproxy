package proxy

import (
	"bufio"
	"bytes"
	"connpool"
	"encoding/binary"
	"errors"
	"labix.org/v2/mgo/bson"
	"mylog"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

const (
	DefaultBufferSize = 16 * 1024

	STATE_INIT       = 0
	STATE_RECIEVE    = 1
	STATE_PARSE      = 2
	STATE_ROUTE      = 3
	STATE_FETCH_CONN = 4
	STATE_SEND_MONGO = 5
	STATE_READ_MONGO = 6
	STATE_RESPONSE   = 7
	STATE_ERR        = -1

	//OP_REPLY = 1
	OP_MSG          = 1000
	OP_UPDATE       = 2001
	OP_INSERT       = 2002
	RESERVED        = 2003
	OP_QUERY        = 2004
	OP_GET_MORE     = 2005
	OP_DELETE       = 2006
	OP_KILL_CURSORS = 2007
)

type MongoHeader struct {
	messageLength int32
	requestID     int32
	responseTo    int32
	opCode        int32
}

type ResponseBody struct {
	responseFlags  int32
	resCursorID    int64
	startingFrom   int32
	numberReturned int32
}

// client represents one connection, TODO:auth not supported NOW
type Client struct {
	ID       int64
	ClientID string
	ReqCount int64
	State    int64

	// original connection
	net.Conn
	Reader      *bufio.Reader
	Writer      *bufio.Writer
	ConnectTime time.Time
	IdleTime    time.Time

	// mongo related
	HadWrite bool
	HasAuth  bool
	*connpool.MongoConn
	HeaderBuf    []byte
	Header       MongoHeader
	GleHeaderBuf []byte
	GleHeader    MongoHeader
	ReqBody      []byte
	ReqFlag      int32
	CursorID     int64

	// after write, we send getlasterror by default, so buffer it
	ResHeaderBuf []byte
	ResHeader    MongoHeader
	Response     []byte
	ResponseBody
	gleOk bool

	// route related
	m        bson.M
	Ns       string
	ShardKey string
	CmdNs    string

	// cursor cache for getMore
	CursorCache map[int64]string

	RecvHandler

	proxy *Proxy
}

func NewClient(id int64, conn net.Conn, p *Proxy) *Client {
	var remoteHost string
	if conn != nil {
		remoteHost, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}
	c := &Client{
		ID:           id,
		ClientID:     remoteHost,
		Conn:         conn,
		Reader:       bufio.NewReaderSize(conn, DefaultBufferSize),
		Writer:       bufio.NewWriterSize(conn, DefaultBufferSize),
		ConnectTime:  time.Now(),
		IdleTime:     time.Now(),
		HeaderBuf:    make([]byte, 16),
		ResHeaderBuf: make([]byte, 16),
		gleOk:        false,
		proxy:        p,
	}

	return c
}

func (c *Client) GetState() int64 {
	return c.State
}

func (c *Client) SetState(state int64) {
	c.state = state
}

func (c *Client) ErrorHappen() {
	c.State = STATE_ERR
}

func (c *Client) Close() {

}

func (c *Client) Reset() {

}

// loop for handling client's request util close
func (c *Client) LoopHandle() {

	for {
		c.Reset()

		// step1, recieve request
		err = c.RecieveRequest()
		if err != nil {
			mylog.WARNING("WARNING: recieve request failed - %s", err.Error())
			break
		}

		// step2, parse request
		err = c.ParseRequest()
		if err != nil {
			mylog.WARNING("WARNING: parse request failed - %s", err.Error())
			break
		}

		// step3, prev handler, for getlasterror request
		willSkip, err = c.PrevHandle()
		if err != nil {
			mylog.WARNING("WARNING: prev handle failed - %s", err.Error())
			break
		}
		if willSkip == true {
			mylog.DEBUG("this request has skipped")
			continue
		}

		// step4, choose connection
		err = c.ChooseConn()
		if err != nil {
			mylog.WARNING("WARNING: choose connection failed - %s", err.Error())
			break
		}

		// step5, request mongo
		err = c.RequestMongo()
		if err != nil {
			mylog.WARNING("WARNING: get connection for set %s fail - %s", setName, err.Error())
			break
		}

		// step6, post handler, save for GetMore
		c.PostHandle()
	}

	c.ErrorHappen()
	c.Close()
}

// recieve header and body
func (c *Client) RecieveRequest() error {
	c.SetState(STATE_RECIEVE)

	c.Header, err = c.RecvPacket(c.Reader, c.HeaderBuf, c.ReqBody)
	return err
}

// parse from the body, get shardkey related data
func (c *Client) ParseRequest() error {
	c.SetState(STATE_PARSE)

	var reqHandler *RequestHandler
	switch c.Header.opCode {
	case OP_UPDATE:
		reqHandler = new(UpdateHandler)
	case OP_INSERT:
		reqHandler = new(InserHandler)
	case OP_QUERY:
		reqHandler = new(QueryHandler)
	case OP_DELETE:
		reqHandler = new(DeleteHandler)
	case OP_GET_MORE:
		reqHandler = new(GetMoreHandler)
	case OP_KILL_CURSORS:
		reqHandler = new(KillHandler)
	default:
		return errors.New("ParseRequest: unsupport opcode - %u -", c.Header.opCode)
	}

	err = reqHandler.Handle(c)
	if err != nil {
		return errors.New("ParseRequest: parse fail - %s -", err.Error())
	}

	c.assembleCmdNs()

	return nil
}

func (c *Client) PrevHandle() (bool, error) {
	// for getlasterror command
	isCommand := bytes.Contains(c.Ns, "$cmd")
	if isCommand == false {
		return false, nil
	}
	_, ok = c.m["getLastError"]
	if ok == false {
		return false, nil
	}

	// just send back our cached result
	if c.gleOk == false {
		mylog.Warning("no getlasterror response cached")
		// send back a default error
	}
}

func (c *Client) assembleCmdNs() {
	// get db
	dotIndex := bytes.IndexByte(c.Ns, ".")
	c.CmdNs = c.Ns[0:dotIndex]
	c.CmdNs = append(c.CmdNs, ".$cmd")
}

// route the query to the right shard
func (c *Client) QueryShard() (string, error) {
	c.SetState(STATE_ROUTE)
	return c.proxy.routerMgr.QueryShard(c.Ns, c.m)
}

// just get the destination host's connection, mainly used by GetMore
func (c *Client) GetConnFromHost(host string) {
	c.SetState(STATE_FETCH_CONN)

	c.MongoConn, err = c.proxy.connMgr.GetByHost(host)
	if err != nil {
		mylog.WARNING("get connection fail host:%s", host)
		return errors.New("get connection fail")
	}

	return nil
}

// request send to slave or Primary
func (c *Client) IsSlaveOk() bool {
	if c.Header.opCode != OP_QUERY { // GetMore will not come here
		return false
	} else if c.ReqFlag&0x4 == 0 {
		return false
	} else {
		return true
	}
}

// fetch connection given the setname
func (c *Client) GetConnFromSet(setname string) error {
	c.SetState(STATE_FETCH_CONN)

	slaveOk := c.IsSlaveOk()

	c.MongoConn, err = c.proxy.connMgr.Get(setname, slaveOk)
	if err != nil {
		mylog.WARNING("get connection fail set:%s slaveOk:%u", setname, slaveOk)
		return errors.New("get connection fail")
	}
}

func (c *Client) ChooseConn() error {
	if c.Header.opCode == OP_GET_MORE {
		host, err = c.CursorCache[c.CursorID]
		if err != nil {
			mylog.WARNING("WARNING: invalid cursor for this connection %u", c.CursorID)
			break
		}
		err = c.GetConnFromHost(host)
		if err != nil {
			mylog.WARNING("WARNING: get connection for this cursor %u fail", c.CursorID)
			break
		}
	} else {
		// step3, get shard for this request
		var setName string
		setName, err = c.QueryShard()
		if err != nil {
			mylog.WARNING("WARNING: get shard failed - %s", err.Error())
			break
		}

		// step4, get connection for the set
		err = c.GetConnFromSet(setName)
		if err != nil {
			mylog.WARNING("WARNING: get connection for set %s fail - %s", setName, err.Error())
			break
		}
	}

	return nil
}

func (c *Client) RequestMongo() error {
	c.SetState(STATE_SEND_MONGO)

	err = c.MongoConn.Write(c.HeaderBuf)
	if err != nil {
		mylog.WARNING("write header to mongo fail -%s-", err.Error())
		return errors.New("write mongo fail")
	}
	err = c.MongoConn.Write(c.ReqBody)
	if err != nil {
		mylog.WARNING("write body to mongo fail -%s-", err.Error())
		return errors.New("write mongo fail")
	}

	// if we have send write, then send a getlasterror automatic
	if c.Header.opCode == OP_INSERT || c.Header.opCode == OP_UPDATE || c.Header.opCode == OP_DELETE {
		err = c.GetLastError()
		if err != nil {
			mylog.WARNING("send getlasterror fail -%s-", err.Error())
			return errors.New("send getlasterror fail")
		}
	}

	c.SetState(STATE_READ_MONGO)

	// for query, just sendback response, for write, cache the getlasterror result
	c.ResHeader, err = c.RecvPacket(c.MongoConn, c.ResHeaderBuf, c.Response)
	if err != nil {
		return err
	}
	if c.Header.opCode == OP_QUERY || c.Header.opCode == OP_GET_MORE {
		_, err = c.Writer.Write(c.ResHeaderBuf)
		if err != nil {
			return err
		}
		_, err = c.Writer.Write(c.Response)
		if err != nil {
			return err
		}
	} else {
		c.gleOk = true
	}

	return nil
}

type QueryOP struct {
	MongoHeader
	flags          int32
	ns             string
	numberToSkip   int32
	numberToReturn int32
}

func (c *Client) GetLastError() error {
	// assemble query
	var getlasterr QueryOP
	getlasterr.messageLength = 16 + len(c.proxy.GleBson)
	getlasterr.opCode = OP_QUERY
	getlasterr.requestID = c.Header.requestID + 1
	getlasterr.responseTo = 0
	getlasterr.flags = 0
	getlasterr.ns = c.CmdNs
	getlasterr.numberToSkip = 0
	getlasterr.numberToReturn = 1

	err = binary.Write(c.MongoConn, binary.LittleEndian, getlasterr)
	if err != nil {
		return err
	}

	// write body
	err = c.MongoConn.Write(c.proxy.GleBson)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) PostHandle() error {
	respBody = c.Response[0:20]
	b := bytes.NewReader(respBody)
	err := binary.Read(b, binary.LittleEndian, &c.ResonseBody)
	if err != nil {
		mylog.WARNING("WARNING: unserial response body failed - %s", err.Error())
		return err
	}

	// cache cursor id
	if c.ResponseBody.resCursorID != 0 {
		c.CursorCache[c.ResponseBody.resCursorID] = MongoConn.RemoteAddr().String()
	}
	return nil
}

type ClientService struct {
}

func (s *ClientService) Handle(proxy *Proxy, conn net.Conn) {
	mylog.INFO("Client: new client(%s)", conn.RemoteAddr())

	// create new client object
	clientID := atomic.AddInt64(&proxy.clientID, 1)
	c := NewClient(clientID, conn, proxy)

	// Loop handle request
	c.LoopHandle()
}
