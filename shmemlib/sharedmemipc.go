package shmemlib

import (
	"context"
	"encoding/binary"
	pb "sharedmemoryipc/proto"

	memory "github.com/apache/arrow/go/arrow/memory"

	"google.golang.org/protobuf/proto"
)

const (
	INDEXOFFSET  = 0
	ENCLENOFFSET = 4
	DATAOFFSET   = 8
)

func (smp *ShmProvider) initEncoderDecoder(ptr []byte) {
	// leave 4 bytes for the length of the message
	smp.buffer = *memory.NewBufferBytes(smp.ipcBuffer[DATAOFFSET:])
}

// Waits for messages or cancellation
// Writes a 1 to the index to indicate that the message has been read
func (smp *ShmProvider) Receive(ctx context.Context,
	OnNewMessage func([]byte, map[string]string) ([]byte, int, string)) (err error) {

	// loop forever
	smp.bufmu.Lock()
	defer func() {
		smp.bufmu.Unlock()
	}()
	for !smp.closed {

		// Wait for a message
		smp.bufmu.Unlock()
		smp.waitforevent(smp.wrevent)
		smp.bufmu.Lock()
		if smp.closed {
			break
		}

		// Were we woken up prematurely?
		index := binary.LittleEndian.Uint32(smp.ipcBuffer[INDEXOFFSET:])
		if index == 1 {
			continue
		}

		// Process the message
		encodingLen := binary.LittleEndian.Uint32(smp.ipcBuffer[ENCLENOFFSET:])
		smp.buffer.ResizeNoShrink(int(encodingLen))
		request := &pb.ShmemRequestMessage{}
		err = proto.Unmarshal(smp.buffer.Bytes(), request)
		if err != nil {
			return err
		}
		responseData, status, statusMessage := OnNewMessage(request.GetData(), request.GetMetadata())
		response := &pb.ShmemResponseMessage{Data: responseData, Status: int32(status), StatusMessage: statusMessage}

		// Encode the response
		smp.buffer.ResizeNoShrink(0)
		encoding, err := proto.Marshal(response)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(smp.ipcBuffer[INDEXOFFSET:], uint32(1))
		encodingLen = uint32(len(encoding))
		binary.LittleEndian.PutUint32(smp.ipcBuffer[ENCLENOFFSET:], uint32(encodingLen))
		copy(smp.ipcBuffer[DATAOFFSET:], encoding)

		// Signal that we have read the data
		smp.signalevent(smp.rdevent)
	}
	return nil
}

// Send function
// Writes a 1 to the index to indicate that the message has been written
func (smp *ShmProvider) Send(ctx context.Context, data []byte,
	requestMetadata map[string]string) ([]byte, int32, string) {

	// Encode the request data and metadata
	smp.buffer.ResizeNoShrink(0)
	request := pb.ShmemRequestMessage{Data: data, Metadata: requestMetadata}
	encoding, err := proto.Marshal(&request)
	if err != nil {

		return nil, 400, err.Error()
	}
	binary.LittleEndian.PutUint32(smp.ipcBuffer[INDEXOFFSET:], uint32(0))
	encodingLen := uint32(len(encoding))
	binary.LittleEndian.PutUint32(smp.ipcBuffer[ENCLENOFFSET:], uint32(encodingLen))
	copy(smp.ipcBuffer[DATAOFFSET:], encoding)

	// Signal the reader and wait for response
	smp.signalevent(smp.wrevent)
	var response *pb.ShmemResponseMessage
	for {
		smp.waitforevent(smp.rdevent)

		// Read the response
		response = &pb.ShmemResponseMessage{}

		// Were we woken up prematurely?
		index := binary.LittleEndian.Uint32(smp.ipcBuffer[INDEXOFFSET:])
		if index != 0 {
			break
		}
	}

	encodingLen = binary.LittleEndian.Uint32(smp.ipcBuffer[ENCLENOFFSET:])
	smp.buffer.Resize(int(encodingLen))
	err = proto.Unmarshal(smp.buffer.Bytes(), response)
	if err != nil {

		return nil, 400, err.Error()
	}
	responseData := response.GetData()
	status := response.GetStatus()
	statusMessage := response.GetStatusMessage()

	// Return the response
	return responseData, status, statusMessage
}
