// Package server provides a multithreaded TCP interface to a simple key value store.
package server

import (
	"bufio"
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

func handle(logger *log.Logger, clientConn io.ReadWriteCloser, store *kvstore.KVStore, serverConns []net.Conn) {
	logger.Print("opened new client connection")

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
			logger.Print("Read error: ", err)
		}

		buffer += scanner.Text()

		command, err := parseCommand(buffer)

		if command != nil {
			logger.Print("found command: ", buffer)

			response, exit := performCommand(logger, store, command, buffer, serverConns)
			if exit {
				logger.Print("closing connection")
				return
			}

			if response != "" {
				logger.Print("writing response: ", response)
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

func openServerConnections(logger *log.Logger, otherServers []string) ([]net.Conn, error) {
	serverConns := make([]net.Conn, 0, len(otherServers))

	for _, otherServer := range otherServers {
		logger.Print("opening new server connection to ", otherServer)

		conn, err := net.Dial("tcp4", otherServer)
		if err != nil {
			logger.Print(err)

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

func performCommand(logger *log.Logger, store *kvstore.KVStore, request *commandRequest,
	command string, serverConns []net.Conn) (string, bool) {
	// TODO: use fan out/fan in to do this in parallel, wait for acks then return local result

	replicateCommand(logger, request, command, serverConns)
	return performCommandLocally(store, request)
}

func replicateCommand(logger *log.Logger, request *commandRequest, command string, serverConns []net.Conn) {
	// only replicate commands that change data
	if request.command == putCommand || request.command == deleteCommand {
		buffer := make([]byte, 3)

		for _, serverConn := range serverConns {
			logger.Print("replicating command to peer: ", command)
			writeResponse(serverConn, command)

			// block until read response, have to assume a single read will get it
			num, err := serverConn.Read(buffer)
			if err != nil {
				logger.Print("error reading peer reply: ", err)
				// continue, peer might recover?
			}

			response := string(buffer[:num])
			logger.Print("received peer reply: ", response)
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
