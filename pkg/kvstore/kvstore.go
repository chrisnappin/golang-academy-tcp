// Package kvstore provides a thread-safe key value store.
package kvstore

// KVStore is a thread-safe key value store.
type KVStore struct {
	data           map[string]string
	requestChannel chan *operationRequest
}

type operation int

const (
	readOperation   operation = iota
	writeOperation  operation = iota
	deleteOperation operation = iota
	closeOperation  operation = iota
)

type operationRequest struct {
	op              operation
	key             string
	value           string
	responseChannel chan<- *operationResponse
}

type operationResponse struct {
	value   string
	present bool
}

// NewKVStore returns a new key value store instance.
func NewKVStore() *KVStore {
	store := &KVStore{
		make(map[string]string),
		make(chan *operationRequest),
	}

	// start the internal go routine
	handleStoreOperations(store)

	return store
}

// Close shuts down the key value store cleanly.
func Close(s *KVStore) {
	s.requestChannel <- &operationRequest{closeOperation, "", "", nil}
}

// Read returns the value of the specified key, and a flag indicating if the key was present.
func Read(s *KVStore, key string) (string, bool) {
	responseChannel := make(chan *operationResponse)
	s.requestChannel <- &operationRequest{readOperation, key, "", responseChannel}

	response := <-responseChannel

	return response.value, response.present
}

// Write sets or updates the key value.
func Write(s *KVStore, key string, value string) {
	responseChannel := make(chan *operationResponse)
	s.requestChannel <- &operationRequest{writeOperation, key, value, responseChannel}

	<-responseChannel
}

// Delete removes a key (if present).
func Delete(s *KVStore, key string) {
	responseChannel := make(chan *operationResponse)
	s.requestChannel <- &operationRequest{deleteOperation, key, "", responseChannel}

	<-responseChannel
}

// handleStoreOperations provides thread-safety for the key value store, by performing operations
// on the store in a single go routine in serial, with input provided through messages on a channel.
func handleStoreOperations(store *KVStore) {
	go func() {
		for {
			request := <-store.requestChannel
			switch request.op {
			case readOperation:
				// read key, if present
				value, present := store.data[request.key]
				request.responseChannel <- &operationResponse{value, present}

			case writeOperation:
				// add or update key
				store.data[request.key] = request.value
				request.responseChannel <- &operationResponse{"", false}

			case deleteOperation:
				// delete key, does nothing if not present
				delete(store.data, request.key)
				request.responseChannel <- &operationResponse{"", false}

			case closeOperation:
				return
			}
		}
	}()
}
