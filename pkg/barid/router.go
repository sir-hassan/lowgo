package barid

import (
	"fmt"
	"sync"
)

// Router forwards messages from a set of source pipes to addressed drain pipes.
//
// The Code field of every message selects the destination drain channel.
// Messages with an unknown destination code are discarded.
// Run blocks until every source channel is closed, then closes all drains and
// returns.
type Router struct {
	sources         []Pipe
	drains          []Pipe
	routes          map[byte]chan<- Frame
	frameBufferSize int

	startOnce sync.Once
	runWG     sync.WaitGroup
}

// New validates the provided endpoints and constructs a Router.
func New(sources []Pipe, drains []Pipe, frameBufferSize int) (*Router, error) {
	if frameBufferSize < 0 {
		panic("barid: frame buffer size cannot be negative")
	}

	seenSources := make(map[byte]struct{}, len(sources))
	for _, source := range sources {
		if source.Channel == nil {
			return nil, ErrNilPipeChannel
		}
		if _, ok := seenSources[source.Code]; ok {
			return nil, fmt.Errorf("%w: 0x%02x", ErrDuplicatePipeCode, source.Code)
		}
		seenSources[source.Code] = struct{}{}
	}

	routes := make(map[byte]chan<- Frame, len(drains))
	for _, drain := range drains {
		if drain.Channel == nil {
			return nil, ErrNilPipeChannel
		}
		if _, ok := routes[drain.Code]; ok {
			return nil, fmt.Errorf("%w: 0x%02x", ErrDuplicatePipeCode, drain.Code)
		}
		routes[drain.Code] = drain.Channel
	}

	return &Router{
		// Copy the caller-provided slice headers so later append/re-slice
		// operations by the caller cannot mutate the router's registration.
		sources: append([]Pipe(nil), sources...),
		// Starting from a nil slice forces append to allocate a distinct backing
		// array while preserving nil/empty semantics.
		drains:          append([]Pipe(nil), drains...),
		routes:          routes,
		frameBufferSize: frameBufferSize,
	}, nil
}

// Run forwards messages until all sources are closed, then closes all drains.
func (r *Router) Run() {
	defer r.closeDrains()

	if len(r.sources) == 0 {
		return
	}

	inbound := make(chan Frame, r.frameBufferSize)

	var wg sync.WaitGroup
	wg.Add(len(r.sources))

	for _, source := range r.sources {
		go func(ch <-chan Frame) {
			defer wg.Done()
			for message := range ch {
				inbound <- message
			}
		}(source.Channel)
	}

	go func() {
		wg.Wait()
		close(inbound)
	}()

	for message := range inbound {
		out, ok := r.routes[message.Code]
		if !ok {
			continue
		}

		out <- message
	}
}

func (r *Router) Start() {
	r.startOnce.Do(func() {
		r.runWG.Add(1)
		go func() {
			defer r.runWG.Done()
			r.Run()
		}()
	})
}

// Wait blocks until a Start-launched Run invocation finishes.
func (r *Router) Wait() {
	r.runWG.Wait()
}

func (r *Router) closeDrains() {
	for _, drain := range r.drains {
		close(drain.Channel)
	}
}
