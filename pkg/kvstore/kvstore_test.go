package kvstore_test

import (
	"tcp/pkg/kvstore"
	"testing"
)

const key1 = "key1"
const value1 = "ABC"
const value2 = "DEF"

func TestEmptyStoreRead(t *testing.T) {
	store := kvstore.NewKVStore()

	value, ok := kvstore.Read(store, key1)
	if ok {
		t.Fatalf("Should have been empty but was: %t value %s", ok, value)
	}

	kvstore.Close(store)
}

func TestSimpleReadAndWrite(t *testing.T) {
	store := kvstore.NewKVStore()

	kvstore.Write(store, key1, value1)

	value, ok := kvstore.Read(store, key1)
	if !ok {
		t.Fatalf("Key should have been present but was: %t (value %s)", ok, value)
	}
	if value != value1 {
		t.Fatalf("Key value should have been %s but was: %s", value1, value)
	}

	kvstore.Close(store)
}

func TestUpdate(t *testing.T) {
	store := kvstore.NewKVStore()

	kvstore.Write(store, key1, value1)

	kvstore.Write(store, key1, value2) // update value

	value, ok := kvstore.Read(store, key1)
	if !ok {
		t.Fatalf("Key should have been present but was: %t (value %s)", ok, value)
	}
	if value != value2 {
		t.Fatalf("Key value should have been %s but was: %s", value2, value)
	}

	kvstore.Close(store)
}

func TestEmptyStoreDelete(t *testing.T) {
	store := kvstore.NewKVStore()

	kvstore.Delete(store, key1) // key not present

	kvstore.Close(store)
}

func TestDelete(t *testing.T) {
	store := kvstore.NewKVStore()

	kvstore.Write(store, key1, value1)

	kvstore.Delete(store, key1)

	value, ok := kvstore.Read(store, key1)
	if ok {
		t.Fatalf("Key should not be present but was: %t (value %s)", ok, value)
	}

	kvstore.Close(store)
}
