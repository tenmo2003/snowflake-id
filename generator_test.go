package snowflakeid

import (
	"sync"
	"testing"
	"time"
)

func mustPanic(t *testing.T, fn func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()

	fn()
}

func decodeID(id int64) (ts int64, machineID int64, seq int64) {
	seq = id & MAX_SEQUENCE
	machineID = (id >> SEQUENCE_BIT) & MAX_MACHINE_ID
	ts = id >> (SEQUENCE_BIT + MACHINE_ID_BIT)
	return ts, machineID, seq
}

func TestNewGenerator_PanicsWhenMachineIDTooLarge(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	mustPanic(t, func() {
		_ = NewGenerator(epoch, MAX_MACHINE_ID+1)
	})
}

func TestGenerateID_EncodesMachineID(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	const machineID = int64(42)
	gen := NewGenerator(epoch, machineID)

	id := gen.GenerateID()
	_, gotMachineID, _ := decodeID(id)
	if gotMachineID != machineID {
		t.Fatalf("machineID decode mismatch: got=%d want=%d", gotMachineID, machineID)
	}
}

func TestGenerateID_UniqueSequential(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	gen := NewGenerator(epoch, 1)

	const n = 10_000
	seen := make(map[int64]struct{}, n)
	for i := 0; i < n; i++ {
		id := gen.GenerateID()
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate id generated: %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestGenerateID_UniqueConcurrent(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	gen := NewGenerator(epoch, 7)

	const (
		goroutines   = 1_000_000
		idsPerWorker = 2_000
	)

	ids := make(chan int64, goroutines*idsPerWorker)
	wg := sync.WaitGroup{}
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerWorker; j++ {
				ids <- gen.GenerateID()
			}
		}()
	}

	wg.Wait()
	close(ids)

	seen := make(map[int64]struct{}, goroutines*idsPerWorker)
	for id := range ids {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate id generated: %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestGenerateID_SequenceIncrementsWithinSameMillisecond(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	gen := NewGenerator(epoch, 3)

	prevID := gen.GenerateID()
	prevTS, _, prevSeq := decodeID(prevID)

	foundSameTS := false
	for i := 0; i < 50_000; i++ {
		id := gen.GenerateID()
		ts, _, seq := decodeID(id)

		if ts == prevTS {
			foundSameTS = true
			want := (prevSeq + 1) & MAX_SEQUENCE
			if seq != want {
				t.Fatalf("sequence increment mismatch: got=%d want=%d", seq, want)
			}
			return
		}

		prevTS, prevSeq = ts, seq
	}

	if !foundSameTS {
		t.Fatalf("did not observe two IDs in the same millisecond; cannot assert sequence increment")
	}
}

func TestGenerateID_PanicsWhenEpochInFuture(t *testing.T) {
	epoch := time.Now().Add(1 * time.Hour)
	gen := NewGenerator(epoch, 1)
	mustPanic(t, func() {
		_ = gen.GenerateID()
	})
}

func TestGenerateID_PanicsWhenClockMovesBackwards(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	gen := NewGenerator(epoch, 1)

	gen.lastGeneratedTimestamp = gen.timestamp() + 10_000
	mustPanic(t, func() {
		_ = gen.GenerateID()
	})
}
