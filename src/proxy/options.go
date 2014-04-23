package proxy

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"
)

type proxyOptions struct {
	// server options
	ID            int64
	Verbose       bool
	ProxyAddress  string
	ManageAddress string
	StatAddress   string
	connPoolSize  int64

	// request options
	MaxReqSize    int64
	MaxResSize    int64
	ClientTimeout time.Duration
	MongoTimeout  time.Duration

	// router cluster options
	RouterAddress       string
	RouterCheckInterval time.Duration
	TopoCheckInterval   time.Duration
}

func NewProxyOptions() *proxyOptions {
	o := &proxyOptions{
		ProxyAddress:  "0.0.0.0:4000",
		ManageAddress: "0.0.0.0:4001",
		StatAddress:   "0.0.0.0:4002",
		connPoolSize:  30,
		MaxReqSize:    16 * 1024 * 1024,
		MaxResSize:    16 * 1024 * 1024,
		ClientTimeout: 5 * time.Second,
		MongoTimeour:  5 * time.Second,

		RouterAddress:       "0.0.0.0:5000,0.0.0.5001,0.0.0.0:5002",
		RouterCheckInterval: 1 * time.Second,
		TopoCheckInterval:   10 * time.Second,
	}

	hostname, err := os.Hostname()
	if err != nil {
		mylog.Fatal("FATAL: get Hostname failed - %s", err)
	}
	h := md5.New()
	io.WriteString(h, hostname)
	o.ID = int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return o
}
