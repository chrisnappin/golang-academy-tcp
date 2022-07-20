package main

import (
	"errors"
	"io"
	"log"
	"net"
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
	log.Println("Starting test harness...")

	// start 3 servers
	go server.StartServer(kvstore.NewKVStore(), server1, peer1, []string{peer2, peer3})
	go server.StartServer(kvstore.NewKVStore(), server2, peer2, []string{peer1, peer3})
	go server.StartServer(kvstore.NewKVStore(), server3, peer3, []string{peer1, peer2})

	// create 3 clients
	client1 := openClientConn(server1)
	client2 := openClientConn(server2)
	client3 := openClientConn(server3)

	defer func() {
		_ = client1.Close()
		_ = client2.Close()
		_ = client3.Close()
	}()

	// send some test requests, check the responses
	checkRequestResponse(client1, "get11a0", "nil") // get key not present
	checkRequestResponse(client2, "get11a0", "nil") // get key not present
	checkRequestResponse(client3, "get11a0", "nil") // get key not present

	checkRequestResponse(client1, "put12bb13999", "ack")  // put key to server 1
	checkRequestResponse(client1, "get12bb0", "val13999") // get key just written
	checkRequestResponse(client2, "get12bb0", "val13999") // get replicated key
	checkRequestResponse(client3, "get12bb0", "val13999") // get replicated key

	checkRequestResponse(client2, "del12bb", "ack")  // delete the key using server 2
	checkRequestResponse(client2, "get12bb0", "nil") // get key, now not present
	checkRequestResponse(client1, "get12bb0", "nil") // delete replicated
	checkRequestResponse(client3, "get12bb0", "nil") // delete replicated

	checkRequestResponse(client1, "bye", "") // shutdown

	log.Println("Test harness completed, all passed!")
}

func openClientConn(hostnamePort string) net.Conn {
	clientConn, err := net.Dial("tcp4", hostnamePort)
	if err != nil {
		log.Fatal("Unable to connect to server: ", err)
	}

	return clientConn
}

func checkRequestResponse(client net.Conn, request string, expectedResponse string) {
	log.Print("[client] sent ", request)

	numWritten, err := client.Write([]byte(request))
	if err != nil {
		log.Fatal("Error writing request: ", err)
	}

	if numWritten != len(request) {
		log.Printf("Expecting to write %d characters, but only wrote %d", len(request), numWritten)
	}

	buffer := make([]byte, len(expectedResponse))

	numRead, err := client.Read(buffer)
	if err != nil {
		if errors.Is(err, io.EOF) {
			log.Print("Server closed connection")
			return
		}

		log.Fatal("Error reading response: ", err)
	}

	if numRead != len(expectedResponse) {
		log.Printf("Expecting to read %d characters, but only read %d", len(expectedResponse), numRead)
	}

	actualResponse := string(buffer[:numRead])

	log.Print("[client] received ", actualResponse)

	if actualResponse != expectedResponse {
		log.Printf("Expected response %s but got %s", expectedResponse, actualResponse)
	}
}
