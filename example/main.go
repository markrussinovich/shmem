package main

import (
	"context"
	"fmt"
	shmlib "sharedmemoryipc/shmemlib"
	"strconv"
	"sync"
)

var msgIndex int

// onNewMessage is the callback function that is called when a new message is received
func onNewMessage(data []byte, requestMetadata map[string]string) ([]byte, int, string) {
	fmt.Printf("[%d] Read from client: %s\n", msgIndex, string(data))
	clientMessage := "Hello, client!"
	fmt.Printf("[%d] Write to client: %s, 200, OK\n\n", msgIndex, clientMessage)
	msgIndex++
	return []byte(clientMessage), 200, "OK"
}

func serverRoutine(ctx context.Context, shm *shmlib.ShmProvider) {
	fmt.Printf("[server]\n\n")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		shm.Receive(ctx, onNewMessage)
		wg.Done()
	}()

	fmt.Println("Press Enter to terminate.")
	fmt.Scanln()
	shm.Close(&wg)
}

func clientRoutine(ctx context.Context, shm *shmlib.ShmProvider) {
	metadata := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	fmt.Printf("[client]\n\n")
	serverMessage := "Hello, server!"
	fmt.Println("Write to server: " + serverMessage)

	// Send the message to the server
	response, status, statusMessage := shm.Send(ctx, []byte(serverMessage), metadata)
	fmt.Println("Response from server: " + string(response) + ", " + strconv.Itoa(int(status)) + ", " + statusMessage)

	shm.Close(nil)
}

// main is the main function
func main() {
	fmt.Print("Golang shared memory IPC example ")

	// create a shared memory provider
	isserver := false
	ctx := context.Background()
	shm := shmlib.ShmProvider{}
	err := shm.Listen(ctx, "shmipc")
	if err != nil {
		// this is the server because the shared memory
		// does not exist yet
		err := shm.Dial(ctx, "shmipc", 100)
		if err != nil {
			fmt.Println("Dial failed:" + err.Error())
			return
		}
		isserver = true
	}

	// write to or read from shared memory
	if isserver {

		serverRoutine(ctx, &shm)

	} else {

		clientRoutine(ctx, &shm)
	}
}
