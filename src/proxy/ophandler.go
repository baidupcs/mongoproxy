package proxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"labix.org/v2/mgo/bson"
	"mylog"
	"strings"
)

// RequestHandler handler mongo request
type RequestHandler interface {
	Handle(c *Client) error
}

// not support OP_MSG
type MsgHandler struct {
}

func (h *MsgHandler) Handle(c *Client) error {
	return errors.New("OP_MSG not supported")
}

type UpdateHandler struct {
}

func (h *UpdateHandler) Handle(c *Client) error {
	// get namespace
	nullIndex := bytes.IndexByte(c.ReqBody, "\n")
	if nullIndex <= 4 {
		return errors.New("Parse: no namespace found in request")
	}
	c.Ns = c.ReqBody[4:nullIndex]

	// get flag
	r := bytes.NewReader(c.ReqBody)
	r.Seek(nullIndex, 0)
	err := binary.Read(r, binary.LittleEndian, &c.ReqFlag)
	if err != nil {
		return errors.New("Parse: no flag found in request")
	}

	// get selector
	var selectorSize int32
	err = binary.Read(r, binary.LittleEndian, &selectorSize)
	if err != nil {
		return errors.New("Parse: parse bson fail")
	}
	start := nullIndex + 4
	end := start + selectorSize
	selector := c.ReqBody[start:end]

	err = bson.Unmarshal(selector, c.m)
	if err != nil {
		return errors.New("Parse: parse bson fail")
	}

	return nil
}

type InserHandler struct {
}

func (h *InsertHandler) Handle(c *Client) error {
	// get flag
	r := bytes.NewReader(c.ReqBody)
	r.Seek(nullIndex, 0)
	err := binary.Read(r, binary.LittleEndian, &c.ReqFlag)
	if err != nil {
		return errors.New("Parse: no flag found in request")
	}

	// get namespace
	nullIndex := bytes.IndexByte(c.ReqBody, "\n")
	if nullIndex <= 4 {
		return errors.New("Parse: no namespace found in request")
	}
	c.Ns = c.ReqBody[4:nullIndex]

	// get document
	var selectorSize int32
	err = binary.Read(r, binary.LittleEndian, &selectorSize)
	if err != nil {
		return errors.New("Parse: parse bson fail")
	}
	start := nullIndex
	end := start + selectorSize
	selector := c.ReqBody[start:end]

	err = bson.Unmarshal(selector, c.m)
	if err != nil {
		return errors.New("Parse: parse bson fail")
	}

	// TODO we only support one document's insert
	if r.Len() > 0 {
		return errors.New("Parse: only one document insert allowed")
	}

	return nil
}

type QueryHandler struct {
}

func (h *QueryHandler) Handle(c *Client) error {
	// get flag
	r := bytes.NewReader(c.ReqBody)
	r.Seek(nullIndex, 0)
	err := binary.Read(r, binary.LittleEndian, &c.ReqFlag)
	if err != nil {
		return errors.New("Parse: no flag found in request")
	}

	// get namespace
	nullIndex := bytes.IndexByte(c.ReqBody, "\n")
	if nullIndex <= 4 {
		return errors.New("Parse: no namespace found in request")
	}
	c.Ns = c.ReqBody[4:nullIndex]

	// get document
	var selectorSize int32
	err = binary.Read(r, binary.LittleEndian, &selectorSize)
	if err != nil {
		return errors.New("Parse: parse bson fail")
	}
	start := nullIndex + 8
	end := start + selectorSize
	selector := c.ReqBody[start:end]

	err = bson.Unmarshal(selector, c.m)
	if err != nil {
		return errors.New("Parse: parse bson fail")
	}

	// TODO we only support one document's insert
	if r.Len() > 0 {
		return errors.New("Parse: only one document insert allowed")
	}

	return nil
}

// getMore must use the same cursor query connection
type GetMoreHandler struct {
}

func (h *GetMoreHandler) Handle(c *Client) error {
	// get namespace
	nullIndex := bytes.IndexByte(c.ReqBody, "\n")
	if nullIndex <= 4 {
		return errors.New("Parse: no namespace found in request")
	}
	c.Ns = c.ReqBody[4:nullIndex]

	// get cursor id
	r := bytes.NewReader(c.ReqBody)
	r.Seek(nullIndex+4, 0)
	err := binary.Read(r, binary.LittleEndian, &c.CursorID)
	if err != nil {
		return errors.New("Parse: no cursorid found in request")
	}
}

type DeleteHandler struct {
}

func (h *DeleteHandler) Handle(c *Client) error {
	// get namespace
	nullIndex := bytes.IndexByte(c.ReqBody, "\n")
	if nullIndex <= 4 {
		return errors.New("Parse: no namespace found in request")
	}
	c.Ns = c.ReqBody[4:nullIndex]

	// get flag
	r := bytes.NewReader(c.ReqBody)
	r.Seek(nullIndex, 0)
	err := binary.Read(r, binary.LittleEndian, &c.ReqFlag)
	if err != nil {
		return errors.New("Parse: no flag found in request")
	}

	// get selector
	var selectorSize int32
	err = binary.Read(r, binary.LittleEndian, &selectorSize)
	if err != nil {
		return errors.New("Parse: parse bson fail")
	}
	start := nullIndex + 4
	end := start + selectorSize
	selector := c.ReqBody[start:end]

	err = bson.Unmarshal(selector, c.m)
	if err != nil {
		return errors.New("Parse: parse bson fail")
	}

	return nil
}

// we don't support kill cursor, timeout cursor will be killed by proxy
type KillHandler struct {
}

func (h *KillHandler) Handle(c *Client) error {
	return errors.New("OP_KILL_CURSORS not supported")
}

// only for recieve mongo packet
type RecvHandler struct {
}

func (r *RecvHandler) RecvPacket(reader Reader, headerBuf []byte, body []byte) (MongoHeader, error) {
	_, err = ReadFull(reader, headerBuf)
	if err != nil {
		mylog.WARNING("WARNING: read header failed - %s", err.Error())
		return nil, errors.New("read header failed")
	}

	// get header
	header := new(MongoHeader)
	b := bytes.NewReader(headerBuf)
	err := binary.Read(b, binary.LittleEndian, &header)
	if err != nil {
		mylog.WARNING("WARNING: unserial request header failed - %s", err.Error())
		return nil, errors.New("unserial request header failed")
	}

	// get body
	body = make([]byte, header.messageLength-16)
	_, err = ReadFull(reader, body)
	if err != nil {
		mylog.WARNING("WARNING: read request body failed - %s", err.Error())
		return nil, errors.New("read request body failed")
	}

	return header, nil
}
