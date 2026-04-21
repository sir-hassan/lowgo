package barid

import (
	"sync"
	"sync/atomic"
	"testing"
)

type pusher struct {
	sources []Pipe
	frames  []Frame
	wg      sync.WaitGroup
}

func newPusher(t testing.TB, sources []Pipe, frames []Frame) *pusher {
	p := &pusher{
		sources: sources,
		frames:  frames,
		wg:      sync.WaitGroup{},
	}
	if len(sources) == 0 {
		t.Fatal("no sources")
	}
	if len(p.frames) == 0 {
		t.Fatal("no frames")
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for _, frame := range frames {
			matched := false
			for _, source := range p.sources {
				if source.Code == frame.Code {
					source.Channel <- frame
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("invalid frame code:%d", frame.Code)
			}
		}
		for _, source := range p.sources {
			close(source.Channel)
		}
	}()

	return p
}

func (p *pusher) wait() {
	p.wg.Wait()
}

type puller struct {
	drains      []Pipe
	wg          sync.WaitGroup
	expectCount int
	t           testing.TB

	total atomic.Int64
}

func newPuller(t testing.TB, drains []Pipe, expectCount int) *puller {
	p := &puller{
		drains:      drains,
		wg:          sync.WaitGroup{},
		expectCount: expectCount,
		t:           t,
	}
	if len(drains) == 0 {
		t.Fatal("no drains")
	}

	for _, drain := range p.drains {
		p.wg.Add(1)
		go func(drain Pipe) {
			defer p.wg.Done()
			for frame := range drain.Channel {
				if frame.Code != drain.Code {
					t.Fatalf("invalid frame code: got %d want %d", frame.Code, drain.Code)
				}

				p.total.Add(1)
			}
		}(drain)
	}

	return p
}

func (p *puller) wait() {
	p.wg.Wait()
	if p.total.Load() < int64(p.expectCount) {
		p.t.Fatalf("expected %d frames, got %d", p.expectCount, p.total.Load())
	}
}

func TestNewPusherPushesFramesToMatchingSources(t *testing.T) {
	t.Parallel()

	cA := make(chan Frame, 1)
	cB := make(chan Frame, 1)

	sources := []Pipe{
		{Code: 0xA1, Channel: cA},
		{Code: 0xB2, Channel: cB},
	}
	frames := []Frame{
		{Code: 0xA1, Bytes: []byte{0x01, 0x02}},
		{Code: 0xB2, Bytes: []byte{0x03}},
	}

	p := newPusher(t, sources, frames)
	p.wait()
}

func TestNewPullerAcceptsMatchingDrains(t *testing.T) {
	t.Parallel()

	cA := make(chan Frame, 1)
	cB := make(chan Frame, 1)

	p := newPuller(t, []Pipe{
		{Code: 0xA1, Channel: cA},
		{Code: 0xB2, Channel: cB},
	}, 2)

	cA <- Frame{Code: 0xA1, Bytes: []byte{0x01}}
	cB <- Frame{Code: 0xB2, Bytes: []byte{0x02}}
	close(cA)
	close(cB)

	p.wait()
}

func TestPusherToPuller(t *testing.T) {
	t.Parallel()

	cA := make(chan Frame)
	cB := make(chan Frame)

	sources := []Pipe{
		{Code: 0xA1, Channel: cA},
		{Code: 0xB2, Channel: cB},
	}
	frames := []Frame{
		{Code: 0xA1, Bytes: []byte{0x01, 0x02}},
		{Code: 0xB2, Bytes: []byte{0x03}},
	}
	pusher := newPusher(t, sources, frames)

	puller := newPuller(t, []Pipe{
		{Code: 0xA1, Channel: cA},
		{Code: 0xB2, Channel: cB},
	}, 2)

	puller.wait()
	pusher.wait()
}
