package shmlib

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

type ShmProvider struct {
	name string

	closed  bool
	bufmu   sync.Mutex
	ptr     []byte
	buffer  bytes.Buffer
	encoder *gob.Encoder
	decoder *gob.Decoder

	event   uintptr
	rdevent uintptr
	wrevent uintptr
}

const (
	IPC_CREAT int = 01000
	IPC_RMID  int = 0
	SETVAL    int = 16
	GETVAL    int = 12
)

type semop struct {
	semNum  uint16
	semOp   int16
	semFlag int16
}

func errnoErr(errno syscall.Errno) error {
	switch errno {
	case syscall.Errno(0):
		return nil
	default:
		return error(errno)
	}
}

func Ftok(name string, projectid uint8) (int, error) {
	var stat = syscall.Stat_t{}
	err := syscall.Stat(name, &stat)
	if err != nil {
		return 0, err
	}
	return int(uint(projectid&0xff)<<24 | uint((stat.Dev&0xff)<<16) | (uint(stat.Ino) & 0xffff)), nil
}

func (smp *ShmProvider) openevents(name string, flag int) error {
	key, err := Ftok(name, 0)
	if err != nil {
		return err
	}
	r1, _, err := syscall.Syscall(
		syscall.SYS_SEMGET,
		uintptr(key),
		uintptr(2),
		uintptr(flag))
	if err != syscall.Errno(0) {

		return err
	}
	smp.event = r1
	smp.rdevent = 0
	smp.wrevent = 1
	return nil
}

func (smp *ShmProvider) signalevent(event uintptr) error {
	post := semop{semNum: uint16(event), semOp: 1, semFlag: 0}
	_, _, err := syscall.Syscall(syscall.SYS_SEMOP, uintptr(smp.event),
		uintptr(unsafe.Pointer(&post)), uintptr(1))
	if err != syscall.Errno(0) {

		return errnoErr(err)
	}
	return nil
}

func (smp *ShmProvider) waitforevent(event uintptr) error {
	wait := semop{semNum: uint16(event), semOp: -1, semFlag: 0}
	_, _, err := syscall.Syscall(syscall.SYS_SEMOP, uintptr(smp.event),
		uintptr(unsafe.Pointer(&wait)), uintptr(1))
	if err != syscall.Errno(0) {

		return errnoErr(err)
	}
	return nil
}

func (smp *ShmProvider) Dial(ctx context.Context, name string, len uint64) error {
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	defer f.Close()

	f.Truncate(int64(len))
	ptr, err := syscall.Mmap(int(f.Fd()), 0, int(len), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {

		return err
	}
	err = smp.openevents(name, IPC_CREAT|0600)
	if err != nil {

		smp.Close(nil)
		return err
	}
	smp.name = name
	smp.ptr = ptr
	smp.initEncoderDecoder(smp.ptr)
	return nil
}

func (smp *ShmProvider) Listen(ctx context.Context, name string) error {
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}

	ptr, err := syscall.Mmap(int(f.Fd()), 0, int(stat.Size()), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {

		return err
	}

	err = smp.openevents(name, 0)
	if err != nil {

		return err
	}
	smp.ptr = ptr
	smp.initEncoderDecoder(smp.ptr)
	return nil
}

func (smp *ShmProvider) Close(wg *sync.WaitGroup) error {

	// signal waiting listening goroutine if there is one
	if wg != nil {
		smp.closed = true
		smp.signalevent(smp.wrevent)
		wg.Wait()
	}
	smp.bufmu.Lock()
	defer smp.bufmu.Unlock()

	if smp.ptr != nil {

		syscall.Munmap(smp.ptr)
	}
	if smp.name != "" {

		// this is the server if we created the file and recorded its name
		_, _, _ = syscall.Syscall(syscall.SYS_SEMCTL, uintptr(smp.event),
			uintptr(0), uintptr(IPC_RMID))
		syscall.Unlink(smp.name)
	}
	return nil
}
