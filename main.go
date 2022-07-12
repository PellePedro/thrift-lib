package thrift-lib

import (
	"crypto/tls"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
)

type Protocol int

const (
	BINARY Protocol = iota
	JSON
	SIMPLEJSON
	COMPACT
)

type Option struct {
	Protocol Protocol
	Secure   bool
	Buffered bool
	Framed   bool
}

type ThriftServer struct {
	server *thrift.TSimpleServer
	addr   string
}

func NewDefaultOption() *Option {
	return &Option{
		Protocol: BINARY,
		Secure:   true,
		Buffered: true,
		Framed:   false,
	}
}

var (
	protocolFactoryMap          = make(map[Protocol]thrift.TProtocolFactory)
	bufferedTransportFactoryMap = make(map[bool]thrift.TTransportFactory)
)

func init() {
	protocolFactoryMap[BINARY] = thrift.NewTBinaryProtocolFactoryConf(nil)
	protocolFactoryMap[JSON] = thrift.NewTJSONProtocolFactory()
	protocolFactoryMap[SIMPLEJSON] = thrift.NewTSimpleJSONProtocolFactoryConf(nil)
	protocolFactoryMap[COMPACT] = thrift.NewTCompactProtocolFactoryConf(nil)

	bufferedTransportFactoryMap[true] = thrift.NewTBufferedTransportFactory(8192)
	bufferedTransportFactoryMap[false] = thrift.NewTTransportFactory()
}

func NewThriftServer(addr string, opt *Option, processor thrift.TProcessor) (*ThriftServer, error) {
	protocolFactory, ok := protocolFactoryMap[opt.Protocol]
	if !ok {
		return nil, fmt.Errorf("unknown protocol")
	}

	var transportFactory thrift.TTransportFactory
	cfg := &thrift.TConfiguration{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	transportFactory = bufferedTransportFactoryMap[opt.Buffered]

	if opt.Framed {
		transportFactory = thrift.NewTFramedTransportFactoryConf(transportFactory, cfg)
	}
	var transport thrift.TServerTransport
	var err error
	if opt.Secure {
		serverTLSConf, clientTLSConf, caPEM, err := certsetup()
		_, _ = clientTLSConf, caPEM
		if err != nil {
			return nil, fmt.Errorf("failed to create tls certificate %w", err)
		}
		transport, err = thrift.NewTSSLServerSocket(addr, serverTLSConf)
	} else {
		transport, err = thrift.NewTServerSocket(addr)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create thrift server %w", err)
	}
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	return &ThriftServer{
		server: server,
		addr:   addr,
	}, nil
}

func (server *ThriftServer) start(done chan bool) chan error {
	returnCh := make(chan error, 1)

	go func() {
		fmt.Printf("Starting Thrift server... on %s \n", server.addr)
		err := server.server.Serve()
		if err != nil {
			returnCh <- err
		}
		fmt.Println("Stopping Thrift server")
		close(returnCh)
	}()
	return returnCh
}
