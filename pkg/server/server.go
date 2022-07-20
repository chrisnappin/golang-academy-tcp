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
func StartServer(store *kvstore.KVStore) {
	listener, err := net.Listen("tcp4", "localhost:8000")
	if err != nil {
		panic(err)
	}

	defer func() {
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()

		if err != nil {
			break
		}

		// TODO: catch ctrl-C, shutdown

		go handle(conn, store)
	}
}

func handle(conn io.ReadWriteCloser, store *kvstore.KVStore) {
	log.Print("opened new connection")

	defer func() {
		_ = conn.Close()
	}()

	var buffer string

	scanner := bufio.NewScanner(conn)
	scanner.Split(bufio.ScanBytes)

	for scanner.Scan() {
		err := scanner.Err()
		if err != nil {
			log.Print("Read error: ", err)
		}

		buffer += scanner.Text()

		command, err := parseCommand(buffer)

		if command != nil {
			log.Print("Found a command: ", buffer)

			response, exit := performCommand(store, command)
			if exit {
				log.Print("Closing connection")
				return
			}

			if response != "" {
				writeResponse(conn, response)
			}

			buffer = ""
		}

		if err != nil {
			writeResponse(conn, "err")

			buffer = ""
		}
	}
}

func writeResponse(writer io.Writer, response string) {
	log.Print("Writing response: ", response)

	_, err := writer.Write([]byte(response))
	if err != nil {
		log.Print("Error writing response: ", err)
	}
}

func performCommand(store *kvstore.KVStore, request *commandRequest) (string, bool) {
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
