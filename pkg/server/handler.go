// Package server provides a multithreaded TCP interface to a simple key value store.
package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"tcp/pkg/kvstore"
	"time"
)

const (
	commandTimeout = 500 * time.Millisecond
	closeRequest   = "bye"
	ackResponse    = "ack"
	errorResponse  = "err"
)

func handle(logger *log.Logger, clientConn io.ReadWriteCloser, store *kvstore.KVStore, serverConns []net.Conn) {
	logger.Print("opened new client connection")

	defer func() {
		_ = clientConn.Close()

		for _, serverConn := range serverConns {
			_ = serverConn.Close()
		}
	}()

	var buffer string

	localStoreChannel, responseChannel := initialiseLocalStoreHandler(logger, store)
	peerChannels, ackChannel := initialiseReplicationHandler(logger, serverConns)

	for {
		input, err := reliableRead(clientConn, 1)
		if err != nil {
			logger.Print("Read error: ", err)
		}

		buffer += input

		command, err := parseCommand(buffer)

		if command != nil {
			logger.Print("found command: ", buffer)

			response := performCommand(logger, localStoreChannel, responseChannel, peerChannels, ackChannel, command)
			if response == closeRequest {
				logger.Print("closing connection")
				return
			}

			if response != "" {
				logger.Print("writing response: ", response)
				_ = reliableWrite(clientConn, response)
			}

			buffer = ""
		}

		if err != nil {
			_ = reliableWrite(clientConn, errorResponse)

			buffer = ""
		}
	}
}

func reliableWrite(writer io.Writer, message string) error {
	start := 0

	for {
		numWritten, err := writer.Write([]byte(message[start:]))
		if err != nil {
			return fmt.Errorf("error writing message: %w", err)
		}

		if numWritten+start < len(message) {
			start += numWritten
		} else {
			return nil
		}
	}
}

func reliableRead(reader io.Reader, expected int) (string, error) {
	remaining := expected
	message := ""

	for {
		buffer := make([]byte, remaining)

		numRead, err := reader.Read(buffer)
		message += string(buffer[:numRead])
		remaining -= numRead

		if remaining == 0 {
			return message, nil
		}

		if err != nil {
			return "", fmt.Errorf("error reading message: %w", err)
		}
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

			return nil, fmt.Errorf("error connecting to peer: %w", err)
		}

		serverConns = append(serverConns, conn)
	}

	return serverConns, nil
}

func performCommand(logger *log.Logger, localStoreChannel chan<- *commandRequest, responseChannel <-chan string,
	peerChannels []chan<- *commandRequest, ackChannel <-chan string, request *commandRequest) string {
	// fan out, by sending the request to every channel
	localStoreChannel <- request

	for _, peerChannel := range peerChannels {
		peerChannel <- request
	}

	// request is then processed in parallel, locally and replicating to peers

	// fan in, by waiting for responses (or timeout)
	var response string

	var numAcks int

	for {
		select {
		case <-ackChannel:
			numAcks++

		case r := <-responseChannel:
			response = r

		case <-time.After(commandTimeout):
			logger.Printf("command timed out, received response: %t, received %d acks", response != "", numAcks)

			if response == "" {
				return errorResponse
			}

			return response

		default:
			if numAcks == len(peerChannels) && response != "" {
				logger.Printf("received response and %d acks", numAcks)
				return response
			}
		}
	}
}

func initialiseReplicationHandler(logger *log.Logger, serverConns []net.Conn) (
	[]chan<- *commandRequest, <-chan string) {
	peerChannels := make([]chan<- *commandRequest, len(serverConns))
	ackChannel := make(chan string)

	for i, serverConn := range serverConns {
		channel := make(chan *commandRequest)
		peerChannels[i] = channel

		go func(conn net.Conn) {
			for {
				request := <-channel

				// only replicate commands that change data
				if request.command == putCommand || request.command == deleteCommand {
					logger.Print("replicating command to peer: ", request.originalText)
					_ = reliableWrite(conn, request.originalText)

					// in a proper system we could use the response to know if peers are active, up to date, etc
					response, _ := reliableRead(conn, 3)
					logger.Print("received peer reply: ", response)
				}

				ackChannel <- ackResponse

				if request.command == closeCommand {
					// exit this go routine
					return
				}
			}
		}(serverConn)
	}

	return peerChannels, ackChannel
}

func initialiseLocalStoreHandler(logger *log.Logger, store *kvstore.KVStore) (chan<- *commandRequest, <-chan string) {
	localStoreChannel := make(chan *commandRequest)
	responseChannel := make(chan string)

	go func() {
		for {
			request := <-localStoreChannel
			logger.Printf("local store - received command %v", request)

			var response string

			switch request.command {
			case putCommand:
				kvstore.Write(store, request.key, request.value)

				response = ackResponse

			case getCommand:
				response = handleVariableLengthGet(store, *request)

			case deleteCommand:
				kvstore.Delete(store, request.key)

				response = ackResponse

			case closeCommand:
				kvstore.Close(store)

				response = closeRequest

			default:
				// unknown command
				response = errorResponse
			}

			logger.Printf("local store - sending response %s", response)
			responseChannel <- response

			if request.command == closeCommand {
				// exit this go routine
				return
			}
		}
	}()

	return localStoreChannel, responseChannel
}

func handleVariableLengthGet(store *kvstore.KVStore, request commandRequest) string {
	value, present := kvstore.Read(store, request.key)

	switch {
	case !present:
		return "nil"

	case request.length == 0 || request.length > len(value):
		// return the whole value
		return "val" + formatArgument(value)

	default:
		// return part of the value
		return "val" + formatArgument(value[:request.length])
	}
}
