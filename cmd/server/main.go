package main

import (
	"flag"
	"log"
	"strings"
	"tcp/pkg/kvstore"
	"tcp/pkg/server"
)

func main() {
	log.Println("Starting up...")

	serverHostnamePort := flag.String("server", "localhost:8000",
		"TCP server hostname and port to listen on (for clients)")

	peerHostnamePort := flag.String("peer", "localhost:8001",
		"TCP server hostname and port to listen on (for server peers)")

	otherServers := flag.String("others", "",
		"Comma-separated list of other server hostnames and ports to replicate with")

	flag.Parse()

	store := kvstore.NewKVStore()
	server.StartServer(store, *serverHostnamePort, *peerHostnamePort, strings.Split(*otherServers, ","))

	log.Println("Shutting down...")
	kvstore.Close(store)
}
