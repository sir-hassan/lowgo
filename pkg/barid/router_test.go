package barid

import (
	"errors"
	"slices"
	"sync"
	"testing"
)

func makeRouter(t testing.TB, sources, drains []Pipe, frameBufferSize int) *Router {
	t.Helper()

	router, err := New(sources, drains, frameBufferSize)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	return router
}

func makePipes(count, buffer int) []Pipe {
	pipeSet := make([]Pipe, 0, count)
	for i := range count {
		pipeSet = append(pipeSet, Pipe{
			Code:    byte(i),
			Channel: make(chan Frame, buffer),
		})
	}
	return pipeSet
}

func runRouter(t testing.TB, router *Router, pusher *pusher, puller *puller) {
	t.Helper()

	// The router owns drain closure, so wait for it before joining the puller.
	router.Start()
	pusher.wait()
	router.Wait()
	puller.wait()
}

func TestFrameHelpersHandleShortFrames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		frame     Frame
		wantCode  byte
		wantBytes []byte
	}{
		{name: "zero value", frame: Frame{}, wantCode: 0x00, wantBytes: nil},
		{name: "code only", frame: Frame{Code: 0xA1}, wantCode: 0xA1, wantBytes: nil},
		{name: "payload", frame: Frame{Code: 0xA1, Bytes: []byte{0x01, 0x02}}, wantCode: 0xA1, wantBytes: []byte{0x01, 0x02}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.frame.Code; got != tc.wantCode {
				t.Fatalf("Code = 0x%02x, want 0x%02x", got, tc.wantCode)
			}
			if got := tc.frame.Bytes; !slices.Equal(got, tc.wantBytes) {
				t.Fatalf("Bytes = %v, want %v", got, tc.wantBytes)
			}
		})
	}
}

func TestNewRejectsInvalidEndpoints(t *testing.T) {
	t.Parallel()

	source := make(chan Frame)
	drain := make(chan Frame)

	t.Run("nil source", func(t *testing.T) {
		t.Parallel()

		_, err := New(
			[]Pipe{{Code: 0x01, Channel: nil}},
			[]Pipe{{Code: 0x02, Channel: drain}},
			0,
		)
		if !errors.Is(err, ErrNilPipeChannel) {
			t.Fatalf("expected ErrNilPipeChannel, got %v", err)
		}
	})

	t.Run("nil drain", func(t *testing.T) {
		t.Parallel()

		_, err := New(
			[]Pipe{{Code: 0x01, Channel: source}},
			[]Pipe{{Code: 0x02, Channel: nil}},
			0,
		)
		if !errors.Is(err, ErrNilPipeChannel) {
			t.Fatalf("expected ErrNilPipeChannel, got %v", err)
		}
	})

	t.Run("duplicate source code", func(t *testing.T) {
		t.Parallel()

		_, err := New(
			[]Pipe{
				{Code: 0x01, Channel: make(chan Frame)},
				{Code: 0x01, Channel: make(chan Frame)},
			},
			[]Pipe{{Code: 0x02, Channel: drain}},
			0,
		)
		if !errors.Is(err, ErrDuplicatePipeCode) {
			t.Fatalf("expected ErrDuplicatePipeCode, got %v", err)
		}
	})

	t.Run("duplicate drain code", func(t *testing.T) {
		t.Parallel()

		_, err := New(
			[]Pipe{{Code: 0x01, Channel: source}},
			[]Pipe{
				{Code: 0x02, Channel: make(chan Frame)},
				{Code: 0x02, Channel: make(chan Frame)},
			},
			0,
		)
		if !errors.Is(err, ErrDuplicatePipeCode) {
			t.Fatalf("expected ErrDuplicatePipeCode, got %v", err)
		}
	})
}

func TestRunRoutesAndClosesDrains(t *testing.T) {
	t.Parallel()

	sources := []Pipe{
		{Code: 0xA1, Channel: make(chan Frame, 4)},
		{Code: 0xB2, Channel: make(chan Frame, 4)},
		{Code: 0xFF, Channel: make(chan Frame, 4)},
		{Code: 0x00, Channel: make(chan Frame, 4)},
	}
	drains := []Pipe{
		{Code: 0xA1, Channel: make(chan Frame, 4)},
		{Code: 0xB2, Channel: make(chan Frame, 4)},
	}

	router := makeRouter(t, sources, drains, 0)

	frames := []Frame{
		{Code: 0xA1, Bytes: []byte{0x01, 0x02}},
		{Code: 0xB2, Bytes: []byte{0x03}},
		// These addresses have no matching drain and must be dropped.
		{Code: 0xFF, Bytes: []byte{0x04}},
		{Code: 0x00},
	}

	runRouter(t, router, newPusher(t, sources, frames), newPuller(t, drains, 2))
}

func TestRunWithNoSourcesClosesDrains(t *testing.T) {
	t.Parallel()

	drain := make(chan Frame)

	router := makeRouter(t, nil, []Pipe{{Code: 0x01, Channel: drain}}, 0)
	router.Start()
	router.Wait()

	if _, ok := <-drain; ok {
		t.Fatal("expected drain channel to be closed")
	}
}

func TestRunHandlesConcurrentSources(t *testing.T) {
	t.Parallel()

	const (
		sourceCount       = 4
		messagesPerSource = 1_000
		totalMessages     = sourceCount * messagesPerSource
	)

	sources := makePipes(sourceCount, 32)
	frames := make([]Frame, 0, totalMessages)
	for i, source := range sources {
		for j := range messagesPerSource {
			frames = append(frames, Frame{Code: source.Code, Bytes: []byte{byte(i), byte(j)}})
		}
	}

	drains := makePipes(sourceCount, totalMessages)
	router := makeRouter(t, sources, drains, 0)

	runRouter(t, router, newPusher(t, sources, frames), newPuller(t, drains, totalMessages))
}

func TestRunWithTenPushersAndTenPullers(t *testing.T) {
	t.Parallel()

	const (
		sourceCount       = 10
		drainCount        = 10
		messagesPerSource = 1_000
		totalMessages     = sourceCount * messagesPerSource
	)

	sources := makePipes(sourceCount, 64)
	frames := make([]Frame, 0, totalMessages)
	for sourceID, source := range sources {
		for seq := range messagesPerSource {
			// Spread traffic evenly across drains while keeping every source active.
			code := byte((sourceID + seq) % drainCount)
			frames = append(frames, Frame{
				Code:  code,
				Bytes: []byte{byte(sourceID), byte(seq >> 8), byte(seq)},
			})
			_ = source
		}
	}

	drains := makePipes(drainCount, totalMessages)
	router := makeRouter(t, sources, drains, 0)
	runRouter(t, router, newPusher(t, sources, frames), newPuller(t, drains, totalMessages))
}

func BenchmarkRouterRun(b *testing.B) {
	for _, size := range []int{8, 64, 256} {
		b.Run("payload_"+itoa(size), func(b *testing.B) {
			source := Pipe{Code: 0xAA, Channel: make(chan Frame, 1024)}
			drain := Pipe{Code: 0xAA, Channel: make(chan Frame, 1024)}

			router := makeRouter(b, []Pipe{source}, []Pipe{drain}, 0)

			var drainWG sync.WaitGroup
			drainWG.Add(1)
			go func() {
				defer drainWG.Done()
				for range drain.Channel {
				}
			}()

			done := make(chan struct{})
			go func() {
				defer close(done)
				router.Run()
			}()

			payload := make([]byte, size)
			payload[0] = 0x00

			b.ResetTimer()
			for range b.N {
				source.Channel <- Frame{Code: 0xAA, Bytes: append([]byte(nil), payload...)}
			}
			b.StopTimer()

			close(source.Channel)
			<-done
			drainWG.Wait()
		})
	}
}

func BenchmarkRouterRunTopology(b *testing.B) {
	tests := []struct {
		name        string
		sourceCount int
		drainCount  int
		bufferSize  int
	}{
		{name: "2x2", sourceCount: 2, drainCount: 2, bufferSize: 256},
		{name: "8x8", sourceCount: 8, drainCount: 8, bufferSize: 256},
		{name: "16x8", sourceCount: 16, drainCount: 8, bufferSize: 512},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			sources := makePipes(tc.sourceCount, tc.bufferSize)
			drains := makePipes(tc.drainCount, b.N)
			router := makeRouter(b, sources, drains, 0)

			var drainWG sync.WaitGroup
			drainWG.Add(len(drains))
			for _, drain := range drains {
				go func(ch chan Frame) {
					defer drainWG.Done()
					for range ch {
					}
				}(drain.Channel)
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				router.Run()
			}()

			b.ResetTimer()
			for i := range b.N {
				source := sources[i%len(sources)]
				code := drains[i%len(drains)].Code
				source.Channel <- Frame{
					Code:  code,
					Bytes: []byte{byte(i >> 8), byte(i)},
				}
			}
			b.StopTimer()

			for _, source := range sources {
				close(source.Channel)
			}
			<-done
			drainWG.Wait()
		})
	}
}

func itoa(v int) string {
	switch v {
	case 8:
		return "8"
	case 64:
		return "64"
	case 256:
		return "256"
	default:
		panic("unexpected benchmark size")
	}
}

////////////////////////////////////////////////////////////////////////////////

func Test_RouterRun_SimpleCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sourceCount int
		drainCount  int
		frameCount  int
	}{
		{name: "few", sourceCount: 2, drainCount: 2, frameCount: 8},
		{name: "more sources", sourceCount: 8, drainCount: 3, frameCount: 100},
		{name: "more drains", sourceCount: 3, drainCount: 8, frameCount: 100},
		{name: "many", sourceCount: 16, drainCount: 12, frameCount: 20_000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sources := makePipes(tc.sourceCount, tc.frameCount)
			drains := makePipes(tc.drainCount, tc.frameCount)

			sharedCodeCount := tc.sourceCount
			if tc.drainCount < sharedCodeCount {
				sharedCodeCount = tc.drainCount
			}
			if sharedCodeCount == 0 {
				t.Fatal("shared code count must be positive")
			}

			frames := make([]Frame, 0, tc.frameCount)
			for i := range tc.frameCount {
				// Restrict addresses to the shared source/drain code range so each
				// generated frame can be pushed into a source and routed into a drain.
				code := byte(i % sharedCodeCount)
				frames = append(frames, Frame{
					Code:  code,
					Bytes: []byte{byte(i >> 8), byte(i)},
				})
			}

			router := makeRouter(t, sources, drains, 0)
			runRouter(t, router, newPusher(t, sources, frames), newPuller(t, drains, tc.frameCount))
		})
	}
}

func Benchmark_RouterRun_SimpleCases(b *testing.B) {
	tests := []struct {
		name        string
		sourceCount int
		drainCount  int
		frameCount  int
	}{
		{name: "few", sourceCount: 2, drainCount: 2, frameCount: 1000},
		{name: "more sources", sourceCount: 8, drainCount: 3, frameCount: 1000},
		{name: "more drains", sourceCount: 3, drainCount: 8, frameCount: 1000},
		{name: "many", sourceCount: 16, drainCount: 12, frameCount: 1000},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			sharedCodeCount := tc.sourceCount
			if tc.drainCount < sharedCodeCount {
				sharedCodeCount = tc.drainCount
			}
			if sharedCodeCount == 0 {
				b.Fatal("shared code count must be positive")
			}

			frames := make([]Frame, 0, tc.frameCount)
			for i := range tc.frameCount {
				code := byte(i % sharedCodeCount)
				frames = append(frames, Frame{
					Code:  code,
					Bytes: []byte{byte(i >> 8), byte(i)},
				})
			}
			for range b.N {
				sources := makePipes(tc.sourceCount, tc.frameCount)
				drains := makePipes(tc.drainCount, tc.frameCount)
				router := makeRouter(b, sources, drains, 0)
				runRouter(b, router, newPusher(b, sources, frames), newPuller(b, drains, tc.frameCount))
			}
		})
	}
}

func Test_RouterRun_Allocations(t *testing.T) {
	const (
		sourceCount = 2
		drainCount  = 2
		frameCount  = 20000
		wantAllocs  = 31
	)

	sharedCodeCount := sourceCount
	if drainCount < sharedCodeCount {
		sharedCodeCount = drainCount
	}

	frames := make([]Frame, 0, frameCount)
	for i := range frameCount {
		frames = append(frames, Frame{
			Code:  byte(i % sharedCodeCount),
			Bytes: []byte{byte(i >> 8), byte(i)},
		})
	}

	run := func(tb testing.TB) {
		sources := makePipes(sourceCount, frameCount)
		drains := makePipes(drainCount, frameCount)

		router := makeRouter(tb, sources, drains, 0)
		runRouter(tb, router, newPusher(tb, sources, frames), newPuller(tb, drains, frameCount))
	}

	// Verify behavior once before measuring allocations for the repeated run path.
	run(t)

	allocs := testing.AllocsPerRun(5, func() {
		run(t)
	})
	if allocs != wantAllocs {
		t.Fatalf("allocs/run: want %.2f, got %.2f", float64(wantAllocs), allocs)
	}
}
