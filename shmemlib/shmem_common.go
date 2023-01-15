package shmemlib

import (
	"context"
	"encoding/binary"
	pb "sharedmemoryipc/proto"

	memory "github.com/apache/arrow/go/arrow/memory"

	"google.golang.org/protobuf/proto"
)

func (smp *ShmProvider) initEncoderDecoder(ptr []byte) {
	// leave 4 bytes for the length of the message
	smp.buffer = *memory.NewBufferBytes(smp.ptr[4:])
}

// Waits for messages or cancellation
func (smp *ShmProvider) Receive(ctx context.Context,
	OnNewMessage func([]byte, map[string]string) ([]byte, int, string)) (err error) {

	// loop forever
	for !smp.closed {

		// Wait for a message
		smp.waitforevent(smp.wrevent)
		smp.bufmu.Lock()
		if smp.closed {

			smp.bufmu.Unlock()
			break
		}

		// Process the message
		encodingLen := binary.LittleEndian.Uint32(smp.ptr[0:])
		smp.buffer.ResizeNoShrink(int(encodingLen))
		request := &pb.ShmemRequestMessage{}
		err = proto.Unmarshal(smp.buffer.Bytes(), request)
		if err != nil {

			smp.bufmu.Unlock()
			return err
		}
		responseData, status, statusMessage := OnNewMessage(request.GetData(), request.GetMetadata())
		response := &pb.ShmemResponseMessage{Data: responseData, Status: int32(status), StatusMessage: statusMessage}

		// Encode the response
		smp.buffer.ResizeNoShrink(0)
		encoding, err := proto.Marshal(response)
		if err != nil {

			smp.bufmu.Unlock()
			return err
		}
		encodingLen = uint32(len(encoding))
		binary.LittleEndian.PutUint32(smp.ptr, uint32(encodingLen))
		copy(smp.ptr[4:], encoding)

		// Signal that we have read the data
		smp.signalevent(smp.rdevent)
		smp.bufmu.Unlock()
	}
	return nil
}

// Send function
func (smp *ShmProvider) Send(ctx context.Context, data []byte,
	requestMetadata map[string]string) ([]byte, int32, string) {

	// Encode the request data and metadata
	smp.buffer.ResizeNoShrink(0)
	request := pb.ShmemRequestMessage{Data: data, Metadata: requestMetadata}
	encoding, err := proto.Marshal(&request)
	if err != nil {

		return nil, 400, err.Error()
	}
	encodingLen := uint32(len(encoding))
	binary.LittleEndian.PutUint32(smp.ptr, uint32(encodingLen))
	copy(smp.ptr[4:], encoding)

	// Signal the reader and wait for response
	smp.signalevent(smp.wrevent)
	smp.waitforevent(smp.rdevent)

	// Read the response
	response := &pb.ShmemResponseMessage{}
	encodingLen = binary.LittleEndian.Uint32(smp.ptr[0:])
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
