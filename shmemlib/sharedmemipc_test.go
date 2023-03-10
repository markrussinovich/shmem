package shmemlib

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

var index int = 0

func BenchmarkTest(b *testing.B) {
	ctx := context.Background()
	const name = "sharedmemipc"
	// First, the client creates the IPC mechanism
	shmClient := &ShmProvider{}
	err := shmClient.Dial(ctx, name, 1024)
	if err != nil {
		panic("Dial failed:" + err.Error())
		//return
	}

	// Second, start the listener asynchronously which opens the IPC mechanism
	shmServer := &ShmProvider{}
	err = shmServer.Listen(ctx, name)
	if err != nil {
		panic("Listen failed:" + err.Error())
		//return
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {

		shmServer.Receive(ctx, onBenchmarkNewMessage)
		wg.Done()
	}()
	metadata := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	for i := 0; i < b.N; i++ {
		data, _, _ := shmClient.Send(ctx, []byte(fmt.Sprintf("Hello, server")), metadata)
		if string(data) != "Hello, client!" {
			panic("didn't get data from server")
		}
	}
	shmServer.Close(&wg)
	wg.Wait()
	shmClient.Close(nil)
	index++
}

func onBenchmarkNewMessage(data []byte, metadata map[string]string) ([]byte, int, string) {
	clientMessage := "Hello, client!"
	msgIndex++
	return []byte(clientMessage), 200, "OK"
}

func Test(t *testing.T) {
	fmt.Print("Shared memory IPC example")
	// Start the service provider

	ctx := context.Background()
	const name = "sharedmemipc"
	// First, the client creates the IPC mechanism
	shmClient := &ShmProvider{}
	err := shmClient.Dial(ctx, name, 1024)
	if err != nil {
		panic("Dial failed:" + err.Error())
		//return
	}

	// Second, start the listener asynchronously which opens the IPC mechanism
	shmServer := &ShmProvider{}
	err = shmServer.Listen(ctx, name)
	if err != nil {
		panic("Listen failed:" + err.Error())
		//return
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// function panicked
				if err, ok := r.(error); ok {
					fmt.Println(err)
				} else {
					fmt.Errorf("Panic: %v", r)
				}
			} else {
				// the goroutine func returned
				fmt.Println("Goroutine returned normally")
			}
		}()
		shmServer.Receive(ctx, onNewMessage)
		wg.Done()
	}()

	for i := 0; i < 1000000; i++ {
		metadata := map[string]string{
			"key1": "value1",
			"key2": "value2",
		}
		data, status, statusMessage := shmClient.Send(ctx, []byte(fmt.Sprintf("Hello, server #%d", i)), metadata)
		fmt.Printf("Read from server: %s, %d, %s\n", string(data), status, statusMessage)
		if string(data) != "Hello, client!" {
			panic("didn't get data from server")
		}
	}
	shmServer.Close(&wg)
	wg.Wait()
	shmClient.Close(nil)
}

var msgIndex = 0

// onNewMessage is the callback function that is called when a new message is received
func onNewMessage(data []byte, metadata map[string]string) ([]byte, int, string) {
	fmt.Printf("[%d] Read from client: %s\n", msgIndex, string(data))
	clientMessage := "Hello, client!"
	fmt.Printf("[%d] Write to client: %s, 200, OK\n\n", msgIndex, clientMessage)
	msgIndex++
	return []byte(clientMessage), 200, "OK"
}

func serverRoutine(ctx context.Context, shm *ShmProvider) {
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
