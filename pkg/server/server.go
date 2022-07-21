// Package server provides a multithreaded TCP interface to a simple key value store.
package server

import (
	"io"
	"log"
	"net"
	"os"
	"tcp/pkg/kvstore"
)

// StartServer starts the tcp key value store server.
func StartServer(store *kvstore.KVStore, serverHostnamePort string, peerHostnamePort string, otherServers []string) {
	// client commands are replicated to peers
	go startConnections("server "+serverHostnamePort+" ", store, serverHostnamePort, otherServers)

	// peer commands are not replicated any further
	go startConnections("peer "+peerHostnamePort+" ", store, peerHostnamePort, nil)
}

func startConnections(description string, store *kvstore.KVStore, hostnamePort string, otherServers []string) {
	logger := log.New(os.Stdout, description, log.Ldate|log.Ltime|log.Lshortfile)

	logger.Print("binding server to TCP port ", hostnamePort)

	clientListener, err := net.Listen("tcp4", hostnamePort)
	if err != nil {
		logger.Fatal("Unable to bind to port: ", err)
	}

	defer func() {
		_ = clientListener.Close()
	}()

	for {
		conn, err := clientListener.Accept()

		if err != nil {
			break
		}

		go openConnectionsAndHandle(logger, conn, store, otherServers)
	}
}

func openConnectionsAndHandle(logger *log.Logger, clientConn io.ReadWriteCloser,
	store *kvstore.KVStore, otherServers []string) {
	serverConns, err := openServerConnections(logger, otherServers)
	if err != nil {
		return
	}

	handle(logger, clientConn, store, serverConns)
}
