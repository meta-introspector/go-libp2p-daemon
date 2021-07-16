package test

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/protocol"
)

func TestUnaryCalls(t *testing.T) {
	_, p1, cancel1 := createDaemonClientPair(t)
	_, p2, cancel2 := createDaemonClientPair(t)

	t.Cleanup(func() {
		cancel1()
		cancel2()
	})

	peer1ID, peer1Addrs, err := p1.Identify()
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.Connect(peer1ID, peer1Addrs); err != nil {
		t.Fatal(err)
	}

	var proto protocol.ID = "sqrt"
	p1.NewUnaryHandler(
		proto,
		func(data []byte) ([]byte, error) {
			f := float64FromBytes(data)
			if f < 0 {
				return nil, fmt.Errorf("can't extract square root from negative")
			}

			result := math.Sqrt(f)
			return float64Bytes(result), nil
		},
	)

	t.Run(
		"test correct request",
		func(t *testing.T) {
			reply, err := p2.UnaryCall(peer1ID, proto, float64Bytes(64))
			if err != nil {
				t.Fatal(err)
			}
			result := float64FromBytes(reply)
			t.Logf("remote returned: %f\n", result)
		},
	)

	t.Run(
		"test bad request",
		func(t *testing.T) {
			_, err := p2.UnaryCall(peer1ID, proto, float64Bytes(-64))
			if err == nil {
				t.Fatal("remote should have returned error")
			}
			t.Logf("remote correctly returned error: '%v'\n", err)
		},
	)

	t.Run(
		"test bad proto",
		func(t *testing.T) {
			_, err := p2.UnaryCall(peer1ID, "bad proto", make([]byte, 0))
			if err == nil {
				t.Fatal("expected error")
			}
			t.Logf("remote correctly returned error: '%v'\n", err)
		},
	)
}

// TODO: create benchmark
func TestConcurrentUnaryCalls(t *testing.T) {
	t.Skip("skipping slow test")
	_, p1, cancel1 := createDaemonClientPair(t)
	_, p2, cancel2 := createDaemonClientPair(t)

	t.Cleanup(func() {
		cancel1()
		cancel2()
	})

	peer1ID, peer1Addrs, err := p1.Identify()
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.Connect(peer1ID, peer1Addrs); err != nil {
		t.Fatal(err)
	}

	var proto protocol.ID = "sqrt"
	p1.NewUnaryHandler(
		proto,
		func(data []byte) ([]byte, error) {
			f := float64FromBytes(data)
			if f < 0 {
				return nil, fmt.Errorf("can't extract square root from negative")
			}

			result := math.Sqrt(f)
			return float64Bytes(result), nil
		},
	)

	// TODO: add flags
	numJobs := 1000
	cnt := make(chan struct{})

	for i := 0; i < numJobs; i++ {
		go func(i int) {
			f := float64(i)
			_, err := p2.UnaryCall(peer1ID, proto, float64Bytes(f))
			if err != nil {
				fmt.Println(err)
			}

			cnt <- struct{}{}
		}(i)
	}

	counter := 0
	before := time.Now()
loop:
	for {
		select {
		case <-cnt:
			counter++
			if counter == numJobs {
				break loop
			}
		}
	}

	t.Logf("success: %d jobs competed in %v\n", numJobs, time.Since(before))
}

func float64FromBytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func float64Bytes(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}
