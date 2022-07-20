package main

import (
	"errors"
	"io"
	"log"
	"net"
	"os"
	"tcp/pkg/kvstore"
	"tcp/pkg/server"
)

const (
	server1 = "localhost:8000"
	peer1   = "localhost:8001"

	server2 = "localhost:8002"
	peer2   = "localhost:8003"

	server3 = "localhost:8004"
	peer3   = "localhost:8005"
)

func main() {
	client1Logger := log.New(os.Stdout, "client1 ", log.Ldate|log.Ltime|log.Lshortfile)
	client2Logger := log.New(os.Stdout, "client2 ", log.Ldate|log.Ltime|log.Lshortfile)
	client3Logger := log.New(os.Stdout, "client3 ", log.Ldate|log.Ltime|log.Lshortfile)

	log.Println("Starting test harness...")

	// start 3 servers
	go server.StartServer(kvstore.NewKVStore(), server1, peer1, []string{peer2, peer3})
	go server.StartServer(kvstore.NewKVStore(), server2, peer2, []string{peer1, peer3})
	go server.StartServer(kvstore.NewKVStore(), server3, peer3, []string{peer1, peer2})

	// create 3 clients
	client1 := openClientConn(client1Logger, server1)
	client2 := openClientConn(client2Logger, server2)
	client3 := openClientConn(client3Logger, server3)

	defer func() {
		_ = client1.Close()
		_ = client2.Close()
		_ = client3.Close()
	}()

	// send some test requests, check the responses
	checkRequestResponse(client1Logger, client1, "get11a0", "nil") // get key not present
	checkRequestResponse(client2Logger, client2, "get11a0", "nil") // get key not present
	checkRequestResponse(client3Logger, client3, "get11a0", "nil") // get key not present

	checkRequestResponse(client1Logger, client1, "put12bb13999", "ack")  // put key to server 1
	checkRequestResponse(client1Logger, client1, "get12bb0", "val13999") // get key just written
	checkRequestResponse(client2Logger, client2, "get12bb0", "val13999") // get replicated key
	checkRequestResponse(client3Logger, client3, "get12bb0", "val13999") // get replicated key

	checkRequestResponse(client2Logger, client2, "del12bb", "ack")  // delete the key using server 2
	checkRequestResponse(client2Logger, client2, "get12bb0", "nil") // get key, now not present
	checkRequestResponse(client1Logger, client1, "get12bb0", "nil") // delete replicated
	checkRequestResponse(client3Logger, client3, "get12bb0", "nil") // delete replicated

	checkRequestResponse(client1Logger, client1, "bye", "") // shutdown

	log.Println("Test harness completed, all passed!")
}

func openClientConn(logger *log.Logger, hostnamePort string) net.Conn {
	clientConn, err := net.Dial("tcp4", hostnamePort)
	if err != nil {
		logger.Fatal("Unable to connect to server: ", err)
	}

	return clientConn
}

func checkRequestResponse(logger *log.Logger, client net.Conn, request string, expectedResponse string) {
	logger.Print("sent ", request)

	numWritten, err := client.Write([]byte(request))
	if err != nil {
		logger.Fatal("Error writing request: ", err)
	}

	if numWritten != len(request) {
		logger.Printf("Expecting to write %d characters, but only wrote %d", len(request), numWritten)
	}

	buffer := make([]byte, len(expectedResponse))

	numRead, err := client.Read(buffer)
	if err != nil {
		if errors.Is(err, io.EOF) {
			logger.Print("Server closed connection")
			return
		}

		logger.Fatal("Error reading response: ", err)
	}

	if numRead != len(expectedResponse) {
		logger.Printf("Expecting to read %d characters, but only read %d", len(expectedResponse), numRead)
	}

	actualResponse := string(buffer[:numRead])

	logger.Print("received ", actualResponse)

	if actualResponse != expectedResponse {
		logger.Printf("Expected response %s but got %s", expectedResponse, actualResponse)
	}
}
