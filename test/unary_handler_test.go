package test

import (
// 	"context"
// 	"encoding/binary"
// 	"errors"
// 	"fmt"
// 	"math"
// 	"sync"
// 	"testing"
// 	"time"
//
// 	"github.com/stretchr/testify/require"
//
// 	"github.com/libp2p/go-libp2p-daemon/p2pclient"
// 	"github.com/libp2p/go-libp2p/core/protocol"
// 	ma "github.com/multiformats/go-multiaddr"
)

// f/

// func TestBalancedCall(t *testing.T) {
// 	dmaddr, c1maddr, dir1Closer := getEndpointsMaker(t)(t)
// 	_, c2maddr, dir2Closer := getEndpointsMaker(t)(t)
//
// 	handlerDaemon, closeDaemon := createDaemon(t, dmaddr)
//
// 	handlerClient1, closeClient1 := createClient(t, handlerDaemon.Listener().Multiaddr(), c1maddr)
// 	handlerClient2, closeClient2 := createClient(t, handlerDaemon.Listener().Multiaddr(), c2maddr)
// 	_, callerClient, callerClose := createDaemonClientPair(t)
// 	defer func() {
// 		closeClient1()
// 		closeClient2()
//
// 		closeDaemon()
//
// 		dir1Closer()
// 		dir2Closer()
// 		callerClose()
// 	}()
//
// 	if err := callerClient.Connect(handlerDaemon.ID(), handlerDaemon.Addrs()); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	var proto protocol.ID = "test"
// 	done := make(chan int, 10)
//
// 	if err := handlerClient1.AddUnaryHandler(proto, getNumberedHandler(done, 1, t), true); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	if err := handlerClient2.AddUnaryHandler(proto, getNumberedHandler(done, -1, t), true); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	for i := 0; i < 10; i++ {
// 		_, err := callerClient.CallUnaryHandler(context.Background(), handlerDaemon.ID(), proto, []byte("test"))
// 		if err != nil {
// 			t.Fatal(err.Error())
// 		}
// 	}
//
// 	control := 0
//
// 	for i := 0; i < 10; i++ {
// 		select {
// 		case x := <-done:
// 			control += x
// 		case <-time.After(1 * time.Second):
// 			t.Fatal("timed out waiting for stream result")
// 		}
// 	}
//
// 	if control != 0 {
// 		t.Fatalf("daemon did not balanced handlers %d", control)
// 	}
// }

// func float64FromBytes(bytes []byte) float64 {
// 	bits := binary.LittleEndian.Uint64(bytes)
// 	float := math.Float64frombits(bits)
// 	return float
// }
//
// func float64Bytes(float float64) []byte {
// 	bits := math.Float64bits(float)
// 	bytes := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(bytes, bits)
// 	return bytes
// }
//
// func slowHandler(ctx context.Context, data []byte) ([]byte, error) {
// 	time.Sleep(time.Second * 3)
// 	return nil, nil
// }
//
// func sqrtHandler(ctx context.Context, data []byte) ([]byte, error) {
// 	f := float64FromBytes(data)
// 	if f < 0 {
// 		return nil, fmt.Errorf("can't extract square root from negative")
// 	}
//
// 	result := math.Sqrt(f)
// 	return float64Bytes(result), nil
// }
//
// func getNumberedHandler(ch chan<- int, x int, t *testing.T) p2pclient.UnaryHandlerFunc {
// 	return func(ctx context.Context, data []byte) ([]byte, error) {
// 		t.Logf("numbered handler x = %d", x)
// 		ch <- x
// 		return []byte("test"), nil
// 	}
// }
//
// // Je reprends mon bien où je le trouve
// // https://stackoverflow.com/questions/47969385/go-float-comparison
// func almostEqual(a, b float64) bool {
// 	return math.Abs(a-b) <= 1e-9
// }
