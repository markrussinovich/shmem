package shmemlib

import (
	"context"
	"os"
	"sync"
	"syscall"

	memory "github.com/apache/arrow/go/arrow/memory"
	sema "github.com/dangerousHobo/go-semaphore"
)

type ShmProvider struct {
	name string

	closed    bool
	bufmu     sync.Mutex
	ipcBuffer []byte
	buffer    memory.Buffer

	rdevent sema.Semaphore
	wrevent sema.Semaphore
}

func (smp *ShmProvider) debugevents() {
	/*
		val, _ := smp.rdevent.GetValue()
		fmt.Printf("rdevent: %d\n", val)
		val, _ = smp.wrevent.GetValue()
		fmt.Printf("wrevent: %d\n", val)
	*/
}

func (smp *ShmProvider) openevents(name string) (err error) {
	if err = smp.rdevent.Open(name+string(eventRequestReadySuffix), 0644, 0); err != nil {
		return err
	}

	if err = smp.wrevent.Open(name+string(eventResponseReadySuffix), 0644, 0); err != nil {
		return err
	}
	smp.debugevents()
	return nil
}

func (smp *ShmProvider) signalevent(event sema.Semaphore) error {
	err := event.Post()
	return err
}

func (smp *ShmProvider) waitforevent(event sema.Semaphore) error {
	smp.debugevents()

	err := event.Wait()

	smp.debugevents()
	return err
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
	// need to open and reset, then reopen
	err = smp.openevents(name)
	if err != nil {

		smp.Close(nil)
		return err
	}
	smp.rdevent.Unlink()
	smp.wrevent.Close()
	smp.wrevent.Unlink()
	smp.wrevent.Close()

	err = smp.openevents(name)
	if err != nil {

		smp.Close(nil)
		return err
	}
	smp.name = name
	smp.ipcBuffer = ptr
	smp.initEncoderDecoder(smp.ipcBuffer)
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

	err = smp.openevents(name)
	if err != nil {

		return err
	}
	smp.ipcBuffer = ptr
	smp.initEncoderDecoder(smp.ipcBuffer)
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

	if smp.ipcBuffer != nil {

		syscall.Munmap(smp.ipcBuffer)
	}
	if smp.name != "" {

		// this is the server if we created the file and recorded its name
		smp.rdevent.Close()
		smp.wrevent.Close()
	}
	return nil
}
