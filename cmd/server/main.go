package main

import (
	"log"
	"tcp/pkg/kvstore"
	"tcp/pkg/server"
)

func main() {
	log.Println("Starting up...")

	store := kvstore.NewKVStore()
	server.StartServer(store)

	log.Println("Shutting down...")
	kvstore.Close(store)
}
