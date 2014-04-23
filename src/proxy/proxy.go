// the main mongo proxy logic impliment

package proxy

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"cluster"
	"connpool"
	"mylog"
	"query"
	"router"

	"labix.org/v2/mgo/bson"
)

type TCPHandler interface {
	Handle(proxy *Proxy, conn net.Conn)
}

type Proxy struct {
	clientID int64
	options  *proxyOptions

	// net
	proxyAddr      *net.TCPAddr
	manageAddr     *net.TCPAddr
	statAddr       *net.TCPAddr
	proxyListener  net.Listener
	manageListener net.Listener
	statListener   net.Listener

	// global resources
	connMgr    *ConnectionManager
	clusterMgr *ClusterManager
	routerMgr  *RouterManager
	queryMgr   *QueryEngine

	// timeout mongod
	killChan chan string

	GleBson []byte
}

func NewProxy(options *proxyOptions) *Proxy {
	proxyAddr, err := net.ResolveTCPAddr("tcp", proxyOptions.ProxyAddress)
	if err != nil {
		mylog.Fatal("FATAL: resolve (%s) failed - %s", proxyOptions.ProxyAddress, err)
		return nil
	}

	manageAddr, err := net.ResolveTCPAddr("tcp", proxyOptions.ManageAddress)
	if err != nil {
		mylog.Fatal("FATAL: resolve (%s) failed - %s", proxyOptions.ManageAddress, err)
		return nil
	}

	statAddr, err := net.ResolveTCPAddr("tcp", proxyOptions.StatAddress)
	if err != nil {
		mylog.Fatal("FATAL: resolve (%s) failed - %s", proxyOptions.StatAddress, err)
		return nil
	}

	n := &Proxy{
		options:    proxyOptions,
		proxyAddr:  proxyAddr,
		manageAddr: manageAddr,
		statAddr:   statAddr,
		killChan:   make(chan string, 4096),
	}

	n.clusterMgr = NewClusterManager(proxyOptions.RouterAddress, proxyOptions.TopoCheckInterval)
	n.connMgr = NewConnectionManager(n.clusterMgr, proxyOptions.connPoolSize)
	n.routerMgr = NewRouterManager(proxyOptions.RouterAddress, proxyOptions.RouterCheckInterval)
	n.queryMgr = NewQueryEngine()

	gleRaw := bson.M{"getLastError": 1, "j": true}
	n.GleBson, err = bson.Marshal(gleRaw)
	if err != nil {
		mylog.Fatal("Marshl getlasterror bson fail - %s", err.Error())
		return nil
	}

	return n
}

func (p *Proxy) TCPServer(listener net.Listener, handler TCPHandler) {
	mylog.INFO("TCP: listening on %s", listener.Addr().String())

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				mylog.WARNING("NOTICE: temporary Accept() failure - %s", err.Error())
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				mylog.WARNING("ERROR: listener.Accept() - %s", err.Error())
			}
			break
		}
		go handler.Handle(p, clientConn)
	}

	mylog.INFO("TCP: closing %s", listener.Addr().String())
}

func (p *Proxy) Run() {
	p.clusterMgr.Start()
	p.routerMgr.Start()
	p.connMgr.Start()

	proxyListener, err := net.Listen("tcp", p.proxyAddr.String())
	if err != nil {
		mylog.Fatal("FATAL: listen (%s) failed - %s", p.proxyAddr, err)
	}
	p.proxyListener = proxyListener
	go p.TCPServer(p.proxyListener, ClientService)

	managerListener, err := net.Listen("tcp", p.manageAddr.String())
	if err != nil {
		mylog.Fatal("FATAL: listen (%s) failed - %s", p.manageAddr, err)
	}
	p.managerListener = managerListener
	go p.TCPServer(p.managerListener, MangerService)

	statListener, err := net.Listen("tcp", p.statAddr.String())
	if err != nil {
		mylog.Fatal("FATAL: listen (%s) failed - %s", p.statAddr, err)
	}
	p.statListener = statListener
	go p.TCPServer(p.statListener, StatService)

	mylog.INFO("Proxy start to run")
}
