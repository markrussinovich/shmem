package main

import (
	"fmt"
	shmlib "sharedmemoryipc/lib"
)

func main() {
	fmt.Println("Shared memory example")
	var isserver bool = false
	shm := shmlib.ShmProvider{}
	err := shm.Open("dapr")
	if err != nil {
		err := shm.Create("dapr", 100)
		if err != nil {
			fmt.Println("create failed")
			return
		}
		isserver = true
	}
	defer shm.Close()

	if isserver {

		fmt.Println("Server writing to shared memory")
		shm.Write([]byte("Hello from server"))

	} else {

		data := make([]byte, 256)
		len, _ := shm.Read(data)
		fmt.Println("Read from shared memory: " + string(data[:len]))
	}
}
