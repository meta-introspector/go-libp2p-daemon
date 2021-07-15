package test

import (
	"bytes"
	"testing"

	"github.com/libp2p/go-libp2p-core/protocol"
)

func TestUnaryCalls(t *testing.T) {
	_, p1, _ := createDaemonClientPair(t)
	_, p2, _ := createDaemonClientPair(t)

	protoName := "hello"

	if err := p1.NewUnaryHandler(
		protoName,
		func([]byte) ([]byte, error) {
			return []byte("waddup?"), nil
		},
	); err != nil {
		t.Fatal(err)
	}

	peer1ID, pids, err := p1.Identify()
	if err != nil {
		t.Fatal(err)
	}

	if err := p2.Connect(peer1ID, pids); err != nil {
		t.Fatal("failed to connect:", err)
	}

	response, err := p2.UnaryCall(
		peer1ID,
		[]protocol.ID{protocol.ID(protoName)},
		[]byte("brothas be like: you, george, ain't tha funking kinda hard on you?"),
	)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(response, []byte("waddup?")) {
		t.Errorf("response not equal to expected '%s' != 'waddup?'", string(response))
	}
}
