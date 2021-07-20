package p2pclient

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-daemon/pb"

	ggio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
)

type PCResponseFuture chan *pb.PCResponse

type UnaryHandlerFunc func([]byte) ([]byte, error)

func (u UnaryHandlerFunc) handle(w ggio.Writer, req *pb.PCResponse) {
	result, err := u(req.GetRequestHandling().Data)

	var errMsg []byte
	if err != nil {
		errMsg = []byte(err.Error())
	}

	w.WriteMsg(
		&pb.PCRequest{
			CallId: req.CallId,
			Message: &pb.PCRequest_UnaryResponse{
				UnaryResponse: &pb.CallUnaryResponse{
					Result: result,
					Error:  errMsg,
				},
			},
		},
	)
}

func (c *Client) run(r ggio.Reader, w ggio.Writer) {
	for {
		var resp pb.PCResponse
		r.ReadMsg(&resp)

		result, err := json.Marshal(resp)
		fmt.Println("received from daemon", string(result), err)

		if _, ok := resp.Message.(*pb.PCResponse_RequestHandling); ok {
			proto := protocol.ID(*resp.GetRequestHandling().Proto)

			c.mhandlers.Lock()
			handler, found := c.unaryHandlers[proto]
			if !found {
				w.WriteMsg(makeErrProtoNotFoundMsg(resp.CallId, string(proto)))
			}
			c.mhandlers.Unlock()

			go handler.handle(w, &resp)
			continue
		}

		go func() {
			callID, err := uuid.FromBytes(resp.CallId)
			if err != nil {
				panic(err)
			}

			rC, _ := c.callFutures.LoadOrStore(callID, make(PCResponseFuture))
			rC.(PCResponseFuture) <- &resp
		}()
	}

}

// getPersistentIO ensures persistent daemon connection and returns
// readers and writers to it
func (c *Client) getPersistentWriter() ggio.Writer {
	c.openPersistentConn.Do(
		func() {
			conn, err := c.newControlConn()
			if err != nil {
				panic(err)
			}

			// write persistent connetion upgrade message to control
			w := NewSafeWriter(ggio.NewDelimitedWriter(conn))
			w.WriteMsg(&pb.Request{Type: pb.Request_PERSISTENT_CONN_UPGRADE.Enum()})
			c.pConnWriter = w

			// run persistent stream listener
			r := ggio.NewDelimitedReader(conn, network.MessageSizeMax)
			go c.run(r, c.pConnWriter)

		},
	)

	return c.pConnWriter
}

// getResponse requests a response from the daemon for a given callID
// TODO: add timeout support
func (c *Client) getResponse(callID uuid.UUID) (*pb.PCResponse, error) {
	rc, _ := c.callFutures.LoadOrStore(callID, make(PCResponseFuture))
	defer c.callFutures.Delete(callID)

	response := <-rc.(PCResponseFuture)
	if dErr := response.GetDaemonError(); dErr != nil {
		return nil, newDaemonError(dErr)
	}

	return response, nil
}

// AddUnaryHandler registers unary handlers to daemon
func (c *Client) AddUnaryHandler(proto protocol.ID, handler UnaryHandlerFunc) error {
	w := c.getPersistentWriter()

	callID := uuid.New()

	w.WriteMsg(
		&pb.PCRequest{
			CallId: callID[:],
			Message: &pb.PCRequest_AddUnaryHandler{
				AddUnaryHandler: &pb.AddUnaryHandlerRequest{
					Proto: (*string)(&proto),
				},
			},
		},
	)

	if _, err := c.getResponse(callID); err != nil {
		return err
	}

	c.mhandlers.Lock()
	c.unaryHandlers[proto] = handler
	c.mhandlers.Unlock()

	return nil
}

func (c *Client) CallUnaryHandler(peerID peer.ID, proto protocol.ID, payload []byte) ([]byte, error) {
	w := c.getPersistentWriter()

	callID := uuid.New()

	// both methods don't return any errors
	cid, _ := callID.MarshalBinary()
	pid, _ := peerID.MarshalBinary()

	w.WriteMsg(
		&pb.PCRequest{
			CallId: cid,
			Message: &pb.PCRequest_CallUnary{
				CallUnary: &pb.CallUnaryRequest{
					Peer:  pid,
					Proto: (*string)(&proto),
					Data:  payload,
				},
			},
		},
	)

	response, err := c.getResponse(callID)
	if err != nil {
		return nil, err
	}

	result := response.GetCallUnaryResponse()
	if len(result.Error) != 0 {
		return nil, newP2PHandlerError(result)
	}

	return result.Result, nil
}

func NewSafeWriter(w ggio.Writer) *safeWriter {
	writer := &safeWriter{w, make(chan proto.Message)}
	go writer.run()
	return writer
}

type safeWriter struct {
	w ggio.Writer
	// message queue
	mq chan proto.Message
}

func (sw *safeWriter) run() {
	for msg := range sw.mq {
		if err := sw.w.WriteMsg(msg); err != nil {
			// TODO: or just close the .mq channel?
			panic(err)
		}
	}
}

func (sw *safeWriter) WriteMsg(msg proto.Message) error {
	sw.mq <- msg
	return nil
}

func newDaemonError(dErr *pb.DaemonError) error {
	return &DaemonError{message: *dErr.Message}
}

type DaemonError struct {
	message string
}

func (de *DaemonError) Error() string {
	return de.message
}

func newP2PHandlerError(resp *pb.CallUnaryResponse) error {
	return &P2PHandlerError{message: string(resp.Error)}
}

type P2PHandlerError struct {
	message string
}

func (he *P2PHandlerError) Error() string {
	return he.message
}

func makeErrProtoNotFoundMsg(callID []byte, proto string) *pb.PCRequest {
	return &pb.PCRequest{
		CallId: callID,
		Message: &pb.PCRequest_UnaryResponse{
			UnaryResponse: &pb.CallUnaryResponse{
				Result: make([]byte, 0),
				Error:  []byte(fmt.Sprintf("handler for protocl %s not found", proto)),
			},
		},
	}
}
