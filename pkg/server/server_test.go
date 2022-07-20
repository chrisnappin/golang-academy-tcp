package server

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"tcp/pkg/kvstore"
	"testing"
)

const (
	key   = "an expert from lorem ipsum"
	value = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
		"Sed elementum mi et faucibus sollicitudin. Mauris ac ex sapien. " +
		"Vivamus lacinia posuere sem vitae venenatis. Aliquam erat volutpat. " +
		"Aliquam erat volutpat. In imperdiet velit sit amet sem lacinia " +
		"eleifend. Curabitur ac ex ut magna vehicula mollis sit amet sed " +
		"massa. Nullam auctor nunc elit, a consequat quam tristique non. " +
		"Fusce ut imperdiet dolor. Duis posuere luctus efficitur. Sed " +
		"facilisis massa sit amet leo dignissim consectetur. Aenean vehicula " +
		"est."
)

// to enable logging change ioutil.Discard to os.Stdout.
var testLogger = log.New(ioutil.Discard, "Code under test: ", log.Ldate|log.Ltime|log.Lshortfile)

func Test_handle_HappyPath(t *testing.T) {
	server, client := net.Pipe()
	store := kvstore.NewKVStore()

	go handle(testLogger, server, store, nil)

	checkRequestResponse(t, client, "get11a0", "nil")       // get key not present
	checkRequestResponse(t, client, "put12bb13999", "ack")  // put key
	checkRequestResponse(t, client, "get12bb0", "val13999") // get key just written
	checkRequestResponse(t, client, "del12bb", "ack")       // delete the key
	checkRequestResponse(t, client, "get12bb0", "nil")      // get key, now not present
	checkRequestResponse(t, client, "bye", "")              // shutdown
}

func Test_handle_LargeEntry(t *testing.T) {
	server, client := net.Pipe()
	store := kvstore.NewKVStore()

	go handle(testLogger, server, store, nil)

	checkRequestResponse(t, client, "put226"+key+"3513"+value, "ack")  // put key
	checkRequestResponse(t, client, "get226"+key+"0", "val3513"+value) // get key just written
	checkRequestResponse(t, client, "del226"+key, "ack")               // delete the key
	checkRequestResponse(t, client, "get226"+key+"0", "nil")           // get key, now not present
	checkRequestResponse(t, client, "bye", "")                         // shutdown
}

func Test_handle_VariableLengthGet(t *testing.T) {
	server, client := net.Pipe()
	store := kvstore.NewKVStore()

	go handle(testLogger, server, store, nil)

	checkRequestResponse(t, client, "put11a2200123456789abcdefghij", "ack")    // put 20 chars value
	checkRequestResponse(t, client, "get11a0", "val2200123456789abcdefghij")   // get whole value
	checkRequestResponse(t, client, "get11a15", "val1501234")                  // get first 5 chars
	checkRequestResponse(t, client, "get11a215", "val2150123456789abcde")      // get first 15 chars
	checkRequestResponse(t, client, "get11a230", "val2200123456789abcdefghij") // get > 20 chars, returns whole value
	checkRequestResponse(t, client, "bye", "")                                 // shutdown
}

func Test_handle_Errors(t *testing.T) {
	server, client := net.Pipe()
	store := kvstore.NewKVStore()

	go handle(testLogger, server, store, nil)

	// valid commands intermingled with invalid ones, to test the buffer being wiped
	// and subsequent commands being successfully recognised
	checkRequestResponse(t, client, "get11a0", "nil")      // valid - get key not present
	checkRequestResponse(t, client, "get1xd", "err")       // invalid - get
	checkRequestResponse(t, client, "put12bb13999", "ack") // valid - put key
	checkRequestResponse(t, client, "put11a1xa", "err")    // invalid - put
	checkRequestResponse(t, client, "del12bb", "ack")      // valid - delete
	checkRequestResponse(t, client, "delx1b", "err")       // invalid - delete
	checkRequestResponse(t, client, "get11a0", "nil")      // valid - get key not present
	checkRequestResponse(t, client, "abc", "err")          // invalid - no such command
	checkRequestResponse(t, client, "bye", "")             // shutdown
}

func checkRequestResponse(t *testing.T, client net.Conn, request string, expectedResponse string) {
	t.Helper()

	numWritten, err := client.Write([]byte(request))
	if err != nil {
		t.Error("Error writing request: ", err)
	}

	if numWritten != len(request) {
		t.Errorf("Expecting to write %d characters, but only wrote %d", len(request), numWritten)
	}

	buffer := make([]byte, len(expectedResponse))

	if expectedResponse == "" {
		// client disconnected, check the connection was shut by the server
		_, err = client.Read(buffer)
		if !errors.Is(err, io.EOF) {
			t.Error("Wrong error returned: ", err)
		}

		return
	}

	numRead, err := client.Read(buffer)
	if err != nil {
		t.Error("Error reading response: ", err)
	}

	if numRead != len(expectedResponse) {
		t.Errorf("Expecting to read %d characters, but only read %d", len(expectedResponse), numRead)
	}

	actualResponse := string(buffer[:numRead])
	if actualResponse != expectedResponse {
		t.Errorf("Expected response %s but got %s", expectedResponse, actualResponse)
	}
}
