package shmemlib

import (
	"context"
	"sync"
	"syscall"
	"unicode/utf16"
	"unsafe"

	memory "github.com/apache/arrow/go/arrow/memory"
)

type ShmProvider struct {
	closed  bool
	bufmu   sync.Mutex
	ptr     []byte
	buffer  memory.Buffer
	handle  uintptr
	wrevent uintptr
	rdevent uintptr
}

var (
	kernel32DLL            = syscall.NewLazyDLL("kernel32.dll")
	procCreateFileMappingA = kernel32DLL.NewProc("CreateFileMappingA")
	procOpenFileMappingA   = kernel32DLL.NewProc("OpenFileMappingA")
	procMapViewOfFile      = kernel32DLL.NewProc("MapViewOfFile")
	procUnmapViewOfFile    = kernel32DLL.NewProc("UnmapViewOfFile")
	procVirtualQuery       = kernel32DLL.NewProc("VirtualQuery")
	procCreateEventA       = kernel32DLL.NewProc("CreateEventA")
	procOpenEventA         = kernel32DLL.NewProc("OpenEventA")
	procSetEvent           = kernel32DLL.NewProc("SetEvent")
)

type MemoryBasicInformation struct {
	BaseAddress       uint64
	AllocationBase    uint64
	AllocationProtect uint32
	Alignment1        uint32
	RegionSize        uint64
	State             uint32
	Protect           uint32
	Type              uint32
	Alignment2        uint32
}

const (
	EVENT_MODIFY_STATE = 0x0002
)

// StringToCharPtr converts a Go string into pointer to a null-terminated cstring.
// This assumes the go string is already ANSI encoded.
func StringToCharPtr(str string) *uint8 {
	chars := append([]byte(str), 0) // null terminated
	return &chars[0]
}

// StringToUTF16Ptr converts a Go string into a pointer to a null-terminated UTF-16 wide string.
// This assumes str is of a UTF-8 compatible encoding so that it can be re-encoded as UTF-16.
func StringToUTF16Ptr(str string) *uint16 {
	wchars := utf16.Encode([]rune(str + "\x00"))
	return &wchars[0]
}

// Create signalling event
func (smp *ShmProvider) createevents(name string) error {
	r1, _, err := procCreateEventA.Call(
		uintptr(unsafe.Pointer(nil)),
		uintptr(0),
		uintptr(0),
		uintptr(unsafe.Pointer(StringToCharPtr(name+"-event-rd"))))
	if err != syscall.Errno(0) {
		return err
	}
	smp.rdevent = r1
	r1, _, err = procCreateEventA.Call(
		uintptr(unsafe.Pointer(nil)),
		uintptr(0),
		uintptr(0),
		uintptr(unsafe.Pointer(StringToCharPtr(name+"-event-wr"))))
	if err != syscall.Errno(0) {
		return err
	}
	smp.wrevent = r1

	return syscall.Errno(0)
}

func (smp *ShmProvider) openevents(name string) error {
	r1, _, err := procOpenEventA.Call(
		syscall.SYNCHRONIZE|EVENT_MODIFY_STATE,
		0,
		uintptr(unsafe.Pointer(StringToCharPtr(name+"-event-rd"))))
	if err != syscall.Errno(0) {
		return err
	}
	smp.rdevent = uintptr(r1)
	r1, _, err = procOpenEventA.Call(
		syscall.SYNCHRONIZE|EVENT_MODIFY_STATE,
		0,
		uintptr(unsafe.Pointer(StringToCharPtr(name+"-event-wr"))))
	if err != syscall.Errno(0) {
		return err
	}
	smp.wrevent = uintptr(r1)
	return syscall.Errno(0)

}

func (smp *ShmProvider) waitforevent(event uintptr) {
	syscall.WaitForSingleObject(syscall.Handle(event), syscall.INFINITE)
}

func (smp *ShmProvider) signalevent(event uintptr) {
	procSetEvent.Call(event)
}

func (smp *ShmProvider) closeevents() {
	if smp.rdevent != 0 {
		syscall.CloseHandle(syscall.Handle(smp.rdevent))
	}
	if smp.wrevent != 0 {
		syscall.CloseHandle(syscall.Handle(smp.wrevent))
	}
}

// Creates a file mapping with the specified name and size, and returns a handle to the file mapping.
func (smp *ShmProvider) Dial(ctx context.Context, name string, len uint64) error {

	// Create the file mapping
	r1, _, err := procCreateFileMappingA.Call(
		uintptr(syscall.InvalidHandle),
		uintptr(unsafe.Pointer(nil)),
		uintptr(syscall.PAGE_EXECUTE_READWRITE),
		uintptr(len>>32),
		uintptr(len&0xffffffff),
		uintptr(unsafe.Pointer(StringToCharPtr(name))))
	if err != syscall.Errno(0) {
		return err
	}
	defer func() {
		if err != syscall.Errno(0) {
			syscall.Close(syscall.Handle(r1))
		}
	}()

	// Map the file into memory
	ptr, _, err := procMapViewOfFile.Call(
		uintptr(r1),
		uintptr(syscall.FILE_MAP_READ|syscall.FILE_MAP_WRITE),
		uintptr(0),
		uintptr(0),
		uintptr(len))
	if err != syscall.Errno(0) {
		return err
	}
	defer func() {
		if err != syscall.Errno(0) {
			procUnmapViewOfFile.Call(ptr)
		}
	}()

	// Create the event
	err = smp.createevents(name)
	if err != syscall.Errno(0) {
		return err
	}
	smp.handle = r1
	smp.ptr = unsafe.Slice((*byte)(unsafe.Pointer(ptr)), len)
	smp.initEncoderDecoder(smp.ptr)
	return nil
}

// Opens a file mapping with the specified name, and returns a handle to the file mapping.
func (smp *ShmProvider) Listen(ctx context.Context, name string) error {
	r1, _, err := procOpenFileMappingA.Call(
		uintptr(syscall.FILE_MAP_READ|syscall.FILE_MAP_WRITE),
		uintptr(0),
		uintptr(unsafe.Pointer(StringToCharPtr(name))))
	if err != syscall.Errno(0) {
		return err
	}
	defer func() {
		if err != syscall.Errno(0) {
			syscall.Close(syscall.Handle(r1))
		}
	}()
	ptr, _, err := procMapViewOfFile.Call(
		uintptr(r1),
		uintptr(syscall.FILE_MAP_READ|syscall.FILE_MAP_WRITE),
		uintptr(0),
		uintptr(0),
		uintptr(0))
	if err != syscall.Errno(0) {
		return err
	}
	defer func() {
		if err != syscall.Errno(0) {
			procUnmapViewOfFile.Call(uintptr(unsafe.Pointer(ptr)))
		}
	}()
	var memInfo MemoryBasicInformation
	_, _, err = procVirtualQuery.Call(
		uintptr(ptr),
		uintptr(unsafe.Pointer(&memInfo)),
		uintptr(unsafe.Sizeof(memInfo)))
	if err != syscall.Errno(0) {
		return err
	}

	// Create the event
	err = smp.openevents(name)
	if err != syscall.Errno(0) {
		return err
	}
	smp.handle = r1
	smp.ptr = unsafe.Slice((*byte)(unsafe.Pointer(ptr)), uint64(memInfo.RegionSize))
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

	// cleanup
	defer syscall.CloseHandle(syscall.Handle(smp.handle))
	r1, _, err := procUnmapViewOfFile.Call(uintptr(unsafe.Pointer(&smp.ptr[0])))
	if err != syscall.Errno(0) {
		return err
	}
	if r1 == 0 {
		return syscall.EINVAL
	}
	smp.closeevents()
	return nil
}
