package connpool

import (
	"errors"
	"mylog"
	"net"
	"time"
)

type MongoConn struct {
	net.TCPConn
	Timeout int32
}

func NewMongoConn(raddr *net.TCPAddr, timeout int32) (*MongoConn, error) {
	m := new(MongoConn)
	m.TCPConn, err = net.DialTCP("tcp4", raddr)
	if err != nil {
		mylog.WARNING("NewMongoConn: connect host -%s- fail", raddr.Error())
		return nil, errors.New("Connect Mongo fail")
	}
	m.Timeout = timeout

	return m, nil
}

func (m *MongoConn) Write(body []byte) error {
	m.TCPConn.SetWriteDeadline(time.Now().Add(m.Timeout * time.Second))

	err = m.TCPConn.Write(body)
	if err != nil {
		mylog.WARNING("WriteReq: write body fail -%s-", err.Error())
		return errors.New("Write Mongo fail")
	}

	return nil
}

func (m *MongoConn) ReadRes(res []byte) error {
	m.TCPConn.SetReadDeadline(time.Now().Add(m.Timeout * time.Second))

	_, err = ReadFull(m.TCPConn, res)
	if err != nil {
		mylog.WARNING("ReadRes: read mongo fail -%s-", err.Error())
		return errors.New("Read Mongo fail")
	}

	return nil
}
