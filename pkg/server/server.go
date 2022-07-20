// Package server provides a multithreaded TCP interface to a simple key value store.
package server

import (
	"bufio"
	"io"
	"log"
	"net"
	"tcp/pkg/kvstore"
)

// StartServer starts the tcp key value store server.
func StartServer(store *kvstore.KVStore, serverHostnamePort string, peerHostnamePort string, otherServers []string) {
	go startClientConnections(store, serverHostnamePort, otherServers)
	go startPeerConnections(store, peerHostnamePort)
}

func startClientConnections(store *kvstore.KVStore, serverHostnamePort string, otherServers []string) {
	servername := "[server " + serverHostnamePort + "] "

	log.Print(servername, "binding server to TCP client port")

	clientListener, err := net.Listen("tcp4", serverHostnamePort)
	if err != nil {
		log.Fatal("Unable to bind to port: ", err)
	}

	defer func() {
		_ = clientListener.Close()
	}()

	for {
		conn, err := clientListener.Accept()

		if err != nil {
			break
		}

		// client commands are replicated to peers
		go handle(conn, store, servername, otherServers)
	}
}

func startPeerConnections(store *kvstore.KVStore, peerHostnamePort string) {
	servername := "[server " + peerHostnamePort + "] "

	log.Print(servername, "binding server to TCP peer port")

	peerListener, err := net.Listen("tcp4", peerHostnamePort)
	if err != nil {
		log.Fatal("Unable to bind to port: ", err)
	}

	defer func() {
		_ = peerListener.Close()
	}()

	for {
		conn, err := peerListener.Accept()

		if err != nil {
			break
		}

		// peer commands are not replicated any further
		go handle(conn, store, servername, nil)
	}
}

func handle(clientConn io.ReadWriteCloser, store *kvstore.KVStore, servername string, otherServers []string) {
	log.Print(servername, "opened new client connection")

	serverConns, err := openServerConnections(servername, otherServers)
	if err != nil {
		return
	}

	defer func() {
		_ = clientConn.Close()

		for _, serverConn := range serverConns {
			_ = serverConn.Close()
		}
	}()

	var buffer string

	scanner := bufio.NewScanner(clientConn)
	scanner.Split(bufio.ScanBytes)

	for scanner.Scan() {
		err := scanner.Err()
		if err != nil {
			log.Print("Read error: ", err)
		}

		buffer += scanner.Text()

		command, err := parseCommand(buffer)

		if command != nil {
			log.Print(servername, "found command: ", buffer)

			response, exit := performCommand(servername, store, command, buffer, serverConns)
			if exit {
				log.Print(servername, "closing connection")
				return
			}

			if response != "" {
				log.Print(servername, "writing response: ", response)
				writeResponse(clientConn, response)
			}

			buffer = ""
		}

		if err != nil {
			writeResponse(clientConn, "err")

			buffer = ""
		}
	}
}

func writeResponse(writer io.Writer, response string) {
	_, err := writer.Write([]byte(response))
	if err != nil {
		log.Print("Error writing response: ", err)
	}
}

func openServerConnections(servername string, otherServers []string) ([]net.Conn, error) {
	serverConns := make([]net.Conn, 0, len(otherServers))

	for _, otherServer := range otherServers {
		log.Print(servername, "opening new server connection to ", otherServer)

		conn, err := net.Dial("tcp4", otherServer)
		if err != nil {
			log.Print(err)

			// close any previously successfully opened connections
			for _, conn = range serverConns {
				_ = conn.Close()
			}

			return nil, err
		}

		serverConns = append(serverConns, conn)
	}

	return serverConns, nil
}

func performCommand(servername string, store *kvstore.KVStore, request *commandRequest,
	command string, serverConns []net.Conn) (string, bool) {
	// TODO: use fan out/fan in to do this in parallel, wait for acks then return local result

	replicateCommand(servername, command, serverConns)
	return performCommandLocally(store, request)
}

func replicateCommand(servername string, command string, serverConns []net.Conn) {
	buffer := make([]byte, 1024)

	for _, serverConn := range serverConns {
		log.Print(servername, "replicating command to peer: ", command)
		writeResponse(serverConn, command)

		if command != "bye" {
			// block until read response, have to assume a single read will get it
			num, err := serverConn.Read(buffer)
			if err != nil {
				log.Print(servername, "error reading peer reply: ", err)
				// continue, peer might recover?
			}

			response := string(buffer[:num])
			log.Print(servername, "received peer reply: ", response)
		}
	}
}

func performCommandLocally(store *kvstore.KVStore, request *commandRequest) (string, bool) {
	switch request.command {
	case putCommand:
		kvstore.Write(store, request.key, request.value)
		return "ack", false

	case getCommand:
		value, present := kvstore.Read(store, request.key)
		if !present {
			return "nil", false
		}

		if request.length == 0 || request.length > len(value) {
			// return the whole value
			return "val" + formatArgument(value), false
		}

		// return part of the value
		return "val" + formatArgument(value[:request.length]), false

	case deleteCommand:
		kvstore.Delete(store, request.key)
		return "ack", false

	case closeCommand:
		kvstore.Close(store)
		return "", true
	}

	return "", true
}
