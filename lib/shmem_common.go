package shmlib

import (
	"bytes"
	"context"
	"encoding/gob"
)

func (smp *ShmProvider) initEncoderDecoder(ptr []byte) {
	smp.buffer = *bytes.NewBuffer(smp.ptr)
	smp.encoder = gob.NewEncoder(&smp.buffer)
	smp.decoder = gob.NewDecoder(&smp.buffer)
	//smp.buffer.Reset()
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
		var requestData []byte
		smp.decoder.Decode(&requestData)
		var requestMetadata map[string]string
		smp.decoder.Decode(&requestMetadata)
		responseData, status, statusMessage := OnNewMessage(requestData, requestMetadata)

		// Encode the response
		smp.buffer.Reset()
		smp.encoder.Encode(responseData)
		smp.encoder.Encode(status)
		smp.encoder.Encode(statusMessage)

		// Signal that we have read the data
		smp.signalevent(smp.rdevent)
		smp.bufmu.Unlock()
	}
	return nil
}

// Send function
func (smp *ShmProvider) Send(ctx context.Context, data []byte,
	requestMetadata map[string]string) ([]byte, int, string) {

	// Encode the request data and metadata
	smp.buffer.Reset()
	err := smp.encoder.Encode(data)
	if err != nil {

		return nil, 400, err.Error()
	}
	err = smp.encoder.Encode(requestMetadata)
	if err != nil {

		return nil, 400, err.Error()
	}

	// Signal the reader and wait for response
	smp.signalevent(smp.wrevent)
	smp.waitforevent(smp.rdevent)

	// Read the response
	var responseData []byte
	err = smp.decoder.Decode(&responseData)
	if err != nil {

		return nil, 400, err.Error()
	}
	var status int
	err = smp.decoder.Decode(&status)
	if err != nil {

		return nil, 400, err.Error()
	}
	var statusMessage string
	err = smp.decoder.Decode(&statusMessage)
	if err != nil {

		return nil, 400, err.Error()
	}

	// Return the response
	return responseData, status, statusMessage
}
