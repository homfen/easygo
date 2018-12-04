// +build linux

package netpoll

import (
	"sync"
	"syscall"
)

// EpollEvent represents epoll events configuration bit mask.
type EpollEvent uint32

// EpollEvents that are mapped to epoll_event.events possible values.
const (
	EPOLLIN      = syscall.EPOLLIN
	EPOLLOUT     = syscall.EPOLLOUT
	EPOLLRDHUP   = syscall.EPOLLRDHUP
	EPOLLPRI     = syscall.EPOLLPRI
	EPOLLERR     = syscall.EPOLLERR
	EPOLLHUP     = syscall.EPOLLHUP
	EPOLLET      = 1 << 31
	EPOLLONESHOT = syscall.EPOLLONESHOT

	// _EPOLLCLOSED is a special EpollEvent value the receipt of which means
	// that the epoll instance is closed.
	_EPOLLCLOSED = 0x20
)

// String returns a string representation of EpollEvent.
func (evt EpollEvent) String() (str string) {
	name := func(event EpollEvent, name string) {
		if evt&event == 0 {
			return
		}
		if str != "" {
			str += "|"
		}
		str += name
	}

	name(EPOLLIN, "EPOLLIN")
	name(EPOLLOUT, "EPOLLOUT")
	name(EPOLLRDHUP, "EPOLLRDHUP")
	name(EPOLLPRI, "EPOLLPRI")
	name(EPOLLERR, "EPOLLERR")
	name(EPOLLHUP, "EPOLLHUP")
	name(EPOLLET, "EPOLLET")
	name(EPOLLONESHOT, "EPOLLONESHOT")
	name(_EPOLLCLOSED, "_EPOLLCLOSED")

	return
}

// Epoll represents single epoll instance.
type Epoll struct {
	muAdd   sync.RWMutex
	muMod   sync.RWMutex
	muDel   sync.RWMutex
	muClose sync.RWMutex

	fd       int
	eventFd  int
	closed   bool
	waitDone chan struct{}

	callbacks map[int]func(EpollEvent)
}

// EpollConfig contains options for Epoll instance configuration.
type EpollConfig struct {
	// OnWaitError will be called from goroutine, waiting for events.
	OnWaitError func(error)
}

func (c *EpollConfig) withDefaults() (config EpollConfig) {
	if c != nil {
		config = *c
	}
	if config.OnWaitError == nil {
		config.OnWaitError = defaultOnWaitError
	}
	return config
}

// EpollCreate creates new epoll instance.
// It starts the wait loop in separate goroutine.
func EpollCreate(c *EpollConfig) (*Epoll, error) {
	config := c.withDefaults()

	fd, err := syscall.EpollCreate1(0)
	if err != nil {
		return nil, err
	}

	r0, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, 0, 0)
	if errno != 0 {
		return nil, errno
	}
	eventFd := int(r0)

	// Set finalizer for write end of socket pair to avoid data races when
	// closing Epoll instance and EBADF errors on writing ctl bytes from callers.
	err = syscall.EpollCtl(fd, syscall.EPOLL_CTL_ADD, eventFd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(eventFd),
	})
	if err != nil {
		syscall.Close(fd)
		syscall.Close(eventFd)
		return nil, err
	}

	ep := &Epoll{
		fd:        fd,
		eventFd:   eventFd,
		callbacks: make(map[int]func(EpollEvent)),
		waitDone:  make(chan struct{}),
	}

	// Run wait loop.
	go ep.wait(config.OnWaitError)

	return ep, nil
}

// closeBytes used for writing to eventfd.
var closeBytes = []byte{1, 0, 0, 0, 0, 0, 0, 0}

// Close stops wait loop and closes all underlying resources.
func (ep *Epoll) Close() (err error) {
	ep.muClose.Lock()
	{
		if ep.closed {
			ep.muClose.Unlock()
			return ErrClosed
		}
		ep.closed = true

		if _, err = syscall.Write(ep.eventFd, closeBytes); err != nil {
			ep.muClose.Unlock()
			return
		}
	}
	ep.muClose.Unlock()

	<-ep.waitDone

	if err = syscall.Close(ep.eventFd); err != nil {
		return
	}

	ep.muClose.Lock()
	// Set callbacks to nil preventing long mu.Lock() hold.
	// This could increase the speed of retreiving ErrClosed in other calls to
	// current epoll instance.
	// Setting callbacks to nil is safe here because no one should read after
	// closed flag is true.
	callbacks := ep.callbacks
	ep.callbacks = nil
	ep.muClose.Unlock()

	for _, cb := range callbacks {
		if cb != nil {
			cb(_EPOLLCLOSED)
		}
	}

	return
}

// Add adds fd to epoll set with given events.
// Callback will be called on each received event from epoll.
// Note that _EPOLLCLOSED is triggered for every cb when epoll closed.
func (ep *Epoll) Add(fd int, events EpollEvent, cb func(EpollEvent)) (err error) {
	ev := &syscall.EpollEvent{
		Events: uint32(events),
		Fd:     int32(fd),
	}

	if ep.closed {
		return ErrClosed
	}
	ep.muAdd.Lock()
	if _, has := ep.callbacks[fd]; !has {
		ep.callbacks[fd] = cb
	}
	ep.muAdd.Unlock()

	//syscall.SetNonblock(fd, true)

	return syscall.EpollCtl(ep.fd, syscall.EPOLL_CTL_ADD, fd, ev)
}

// Del removes fd from epoll set.
func (ep *Epoll) Del(fd int) (err error) {

	if ep.closed {
		return ErrClosed
	}
	ep.muDel.Lock()
	if _, ok := ep.callbacks[fd]; !ok {
		ep.muDel.Unlock()
		return ErrNotRegistered
	}

	delete(ep.callbacks, fd)
	ep.muDel.Unlock()

	return syscall.EpollCtl(ep.fd, syscall.EPOLL_CTL_DEL, fd, nil)
}

// Mod sets to listen events on fd.
func (ep *Epoll) Mod(fd int, events EpollEvent) (err error) {
	ev := &syscall.EpollEvent{
		Events: uint32(events),
		Fd:     int32(fd),
	}

	if ep.closed {
		return ErrClosed
	}
	ep.muMod.RLock()
	if _, ok := ep.callbacks[fd]; !ok {
		ep.muMod.RUnlock()
		return ErrNotRegistered
	}
	ep.muMod.RUnlock()

	return syscall.EpollCtl(ep.fd, syscall.EPOLL_CTL_MOD, fd, ev)
}

const (
	maxWaitEventsBegin = 1024
	maxWaitEventsStop  = 32768
)

func (ep *Epoll) wait(onError func(error)) {
	defer func() {
		if err := syscall.Close(ep.fd); err != nil {
			onError(err)
		}
		close(ep.waitDone)
	}()

	events := make([]syscall.EpollEvent, maxWaitEventsBegin)
	for {
		n, err := syscall.EpollWait(ep.fd, events, -1)
		if err != nil {
			if temporaryErr(err) {
				continue
			}
			onError(err)
			return
		}

		for i := 0; i < n; i++ {
			fd := int(events[i].Fd)
			if fd == ep.eventFd { // signal to close
				return
			}
			cb := ep.callbacks[fd]
			cb(EpollEvent(events[i].Events))
		}

		doubleN := n * 2
		if n >= maxWaitEventsBegin && doubleN < maxWaitEventsStop {
			events = make([]syscall.EpollEvent, doubleN)
		} else {
			events = make([]syscall.EpollEvent, n)
		}
	}
}
