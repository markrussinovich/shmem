package shmlib

import (
	"encoding/binary"
	"syscall"
)

type ShmProvider struct {
	data    []byte
	handle  uintptr
	wrevent uintptr
	rdevent uintptr
}

// implements io.Reader
// Reads a length followed by a byte array from the shared memory.
func (smp *ShmProvider) Read(p []byte) (n int, err error) {

	smp.waitforevent(smp.wrevent)
	var length uint32 = binary.BigEndian.Uint32(smp.data[:4])
	len := copy(p, smp.data[4:4+length])
	smp.signalevent(smp.rdevent)
	return len, nil
}

// implements io.Writer
// Writes a length followed by a byte array to the shared memory.
func (smp *ShmProvider) Write(p []byte) (n int, err error) {
	if len(p)+4 > len(smp.data) {
		return 0, syscall.EINVAL
	}
	binary.BigEndian.PutUint32(smp.data[:4], uint32(len(p)))
	len := copy(smp.data[4:], p[:])
	smp.signalevent(smp.wrevent)
	smp.waitforevent(smp.rdevent)
	return len, nil
}
