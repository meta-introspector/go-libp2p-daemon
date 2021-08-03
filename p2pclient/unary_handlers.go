package p2pclient

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-daemon/pb"

	ggio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
)

type PCResponseFuture chan *pb.PCResponse

type UnaryHandlerFunc func(context.Context, []byte) ([]byte, error)

func (u UnaryHandlerFunc) handle(ctx context.Context, w ggio.Writer, req *pb.PCResponse) {
	result, err := u(ctx, req.GetRequestHandling().Data)

	response := &pb.CallUnaryResponse{}
	if err == nil {
		response.Result = &pb.CallUnaryResponse_Response{
			Response: result,
		}
	} else {
		response.Result = &pb.CallUnaryResponse_Error{
			Error: []byte(err.Error()),
		}
	}

	w.WriteMsg(
		&pb.PCRequest{
			CallId: req.CallId,
			Message: &pb.PCRequest_UnaryResponse{
				UnaryResponse: response,
			},
		},
	)
}

func (c *Client) run(r ggio.Reader, w ggio.Writer) {
	for {
		var resp pb.PCResponse
		r.ReadMsg(&resp)

		callID, err := uuid.FromBytes(resp.CallId)
		if err != nil {
			log.Debugw("received response with bad call id:", "error", err)
			continue
		}

		switch resp.Message.(type) {
		case *pb.PCResponse_RequestHandling:
			proto := protocol.ID(*resp.GetRequestHandling().Proto)

			c.mhandlers.Lock()
			handler, found := c.unaryHandlers[proto]
			if !found {
				w.WriteMsg(makeErrProtoNotFoundMsg(resp.CallId, string(proto)))
			}
			c.mhandlers.Unlock()

			go func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				handler.handle(ctx, w, &resp)
			}()

		case *pb.PCResponse_DaemonError, *pb.PCResponse_CallUnaryResponse, *pb.PCResponse_Cancel, nil:
			go func() {
				rC, _ := c.callFutures.LoadOrStore(callID, make(PCResponseFuture))
				rC.(PCResponseFuture) <- &resp
			}()
		}
	}

}

// getPersistentIO ensures persistent daemon connection and returns
// readers and writers to it
func (c *Client) getPersistentWriter() ggio.WriteCloser {
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

func (c *Client) CallUnaryHandler(
	ctx context.Context,
	peerID peer.ID,
	proto protocol.ID,
	payload []byte,
) ([]byte, error) {

	w := c.getPersistentWriter()

	callID := uuid.New()

	// both methods don't return any errors
	cid, _ := callID.MarshalBinary()
	pid, _ := peerID.MarshalBinary()

	done := make(chan struct{})
	go func() {
		defer close(done)

		select {
		case <-done:
			return
		case <-ctx.Done():
			w.WriteMsg(
				&pb.PCRequest{
					CallId:  cid,
					Message: &pb.PCRequest_Cancel{Cancel: &pb.Cancel{}},
				},
			)
		}
	}()

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

	if response.GetCancel() != nil {
		return nil, ctx.Err()
	}

	result := response.GetCallUnaryResponse()
	if len(result.GetError()) != 0 {
		return nil, newP2PHandlerError(result)
	}

	select {
	case done <- struct{}{}:
		return result.GetResponse(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Cancel cancelles a request w/ given id
func (c *Client) Cancel(callID uuid.UUID) error {
	return c.getPersistentWriter().WriteMsg(
		&pb.PCRequest{
			CallId: callID[:],
			Message: &pb.PCRequest_Cancel{
				Cancel: &pb.Cancel{},
			},
		},
	)
}

func NewSafeWriter(w ggio.WriteCloser) *safeWriter {
	return &safeWriter{w: w}
}

type safeWriter struct {
	w ggio.WriteCloser
	m sync.Mutex
}

func (sw *safeWriter) WriteMsg(msg proto.Message) error {
	sw.m.Lock()
	defer sw.m.Unlock()
	return sw.w.WriteMsg(msg)
}

func (sw *safeWriter) Close() error {
	sw.m.Lock()
	defer sw.m.Unlock()

	return sw.w.Close()
}

func newDaemonError(dErr *pb.DaemonError) error {
	return &DaemonError{message: *dErr.Message}
}

type DaemonError struct {
	message string
}

func (de *DaemonError) Error() string {
	return fmt.Sprintf("Daemon failed with %s:", de.message)
}

func newP2PHandlerError(resp *pb.CallUnaryResponse) error {
	return &P2PHandlerError{message: string(resp.GetError())}
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
				Result: &pb.CallUnaryResponse_Error{
					Error: []byte(fmt.Sprintf("handler for protocl %s not found", proto)),
				},
			},
		},
	}
}
