package p2pd

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	ggio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-daemon/pb"
)

func (d *Daemon) handleUpgradedConn(r ggio.Reader, unsafeW ggio.Writer) {
	w := &safeWriter{w: unsafeW}

	for {
		var req pb.PCRequest
		if err := r.ReadMsg(&req); err != nil && err != io.EOF {
			log.Debugw("error reading message", "error", err)
			return
		}

		callID, err := uuid.FromBytes(req.CallId)
		if err != nil {
			log.Debugw("bad call id: ", "error", err)
			continue
		}

		switch req.Message.(type) {
		case *pb.PCRequest_AddUnaryHandler:
			go func() {
				resp := d.doAddUnaryHandler(w, callID, req.GetAddUnaryHandler())
				if err := w.WriteMsg(resp); err != nil {
					log.Debugw("error reading message", "error", err)
					return
				}
			}()

		case *pb.PCRequest_CallUnary:
			go func() {
				ctx, cancel := context.WithCancel(context.Background())
				d.cancelUnary.Store(callID, cancel)
				defer cancel()

				defer d.cancelUnary.Delete(callID)

				resp := d.doUnaryCall(ctx, callID, &req)

				if err := w.WriteMsg(resp); err != nil {
					log.Debugw("error reading message", "error", err)
					return
				}
			}()

		case *pb.PCRequest_UnaryResponse:
			go func() {
				resp := d.doSendReponseToRemote(&req)
				if err := w.WriteMsg(resp); err != nil {
					log.Debugw("error reading message", "error", err)
					return
				}
			}()

		case *pb.PCRequest_Cancel:
			go func() {
				cf, found := d.cancelUnary.Load(callID)
				if !found {
					return
				}

				cf.(context.CancelFunc)()
			}()
		}
	}
}

func (d *Daemon) doAddUnaryHandler(w ggio.Writer, callID uuid.UUID, req *pb.AddUnaryHandlerRequest) *pb.PCResponse {
	// x gon' give it to ya
	d.mx.Lock()
	defer d.mx.Unlock()

	p := protocol.ID(*req.Proto)
	if _, registered := d.registeredUnaryProtocols[p]; registered {
		return errorUnaryCallString(
			callID,
			fmt.Sprintf("handler for protocol %s already set", *req.Proto),
		)
	}

	d.host.SetStreamHandler(p, d.getPersistentStreamHandler(w))

	log.Debugw("set unary stream handler", "protocol", p)

	return okUnaryCallResponse(callID)
}

func (d *Daemon) doUnaryCall(ctx context.Context, callID uuid.UUID, req *pb.PCRequest) *pb.PCResponse {
	// process request
	pid, err := peer.IDFromBytes(req.GetCallUnary().Peer)
	if err != nil {
		return errorUnaryCall(callID, err)
	}

	// open stream to remote
	remoteStream, err := d.host.NewStream(
		ctx,
		pid,
		protocol.ID(*req.GetCallUnary().Proto),
	)
	if err != nil {
		return errorUnaryCall(callID, err)
	}
	defer remoteStream.Close()

	select {
	case response := <-exchangeMessages(ctx, remoteStream, req):
		return response

	case <-ctx.Done():
		return okCancelled(callID)
	}
}

func exchangeMessages(ctx context.Context, s network.Stream, req *pb.PCRequest) <-chan *pb.PCResponse {
	callID, _ := uuid.FromBytes(req.CallId)
	rc := make(chan *pb.PCResponse)

	go func() {
		defer close(rc)

		// write request to remote
		if err := ggio.NewDelimitedWriter(s).WriteMsg(req); ctx.Err() != nil {
			return
		} else if err != nil {
			rc <- errorUnaryCall(callID, err)
			return
		}

		// await response from remote
		remoteResp := &pb.PCRequest{}
		if err := ggio.NewDelimitedReader(s, network.MessageSizeMax).ReadMsg(remoteResp); ctx.Err() != nil {
			return
		} else if err != nil {
			rc <- errorUnaryCall(callID, err)
			return
		}

		// convert response to a persistent channel response
		resp := okUnaryCallResponse(callID)
		resp.Message = &pb.PCResponse_CallUnaryResponse{
			CallUnaryResponse: remoteResp.GetUnaryResponse(),
		}

		// avoid blocking rc channel on cancelled context
		select {
		case rc <- resp:
			return

		case <-ctx.Done():
			return
		}
	}()

	return rc
}

// awaitReadFail writers to a semaphor channel if the given io.Reader fails to
// read before the context was cancelled
func awaitReadFail(ctx context.Context, r io.Reader) <-chan struct{} {
	semaphor := make(chan struct{})

	go func() {
		defer close(semaphor)

		buff := make([]byte, 1)
		if _, err := r.Read(buff); err != nil {
			select {
			case semaphor <- struct{}{}:
			case <-ctx.Done():
			}
		}
	}()

	return semaphor
}

// getPersistentStreamHandler returns a lib-p2p stream handler tied to a
// given persistent client stream
func (d *Daemon) getPersistentStreamHandler(cw ggio.Writer) network.StreamHandler {
	return func(s network.Stream) {
		defer s.Close()

		// read request from remote peer
		req := &pb.PCRequest{}
		if err := ggio.NewDelimitedReader(s, network.MessageSizeMax).ReadMsg(req); err != nil {
			log.Debugw("failed to read proto from incoming p2p stream", "error", err)
			return
		}

		// now the peer field points to the caller
		req.GetCallUnary().Peer = []byte(s.Conn().RemotePeer())

		callID, err := uuid.FromBytes(req.CallId)
		if err != nil {
			log.Debugw("bad call id in p2p handler", "error", err)
			return
		}

		// create response channel
		rc := make(chan *pb.PCRequest)
		d.responseWaiters.Store(callID, rc)
		defer d.responseWaiters.Delete(callID)

		ctx, cancel := context.WithCancel(d.ctx)
		defer cancel()

		// TODO: do we need to check for cancelled context
		// before sending handling request to client?

		// request handling from daemon's client
		resp := &pb.PCResponse{
			CallId: req.CallId,
			Message: &pb.PCResponse_RequestHandling{
				RequestHandling: req.GetCallUnary(),
			},
		}
		if err := cw.WriteMsg(resp); err != nil {
			log.Debugw("failed to write message to client", "error", err)
			return
		}

		select {
		case <-awaitReadFail(ctx, s):
			// tell the client he got cancelled
			if err := cw.WriteMsg(
				&pb.PCResponse{
					CallId: callID[:],
					Message: &pb.PCResponse_Cancel{
						Cancel: &pb.Cancel{},
					},
				},
			); err != nil {
				log.Debugw("failed to write to client", "error", err)
				return
			}
			return
		case response := <-rc:
			w := ggio.NewDelimitedWriter(s)
			if err := w.WriteMsg(response); err != nil {
				log.Debugw("failed to write message to remote", "error", err)
			}
		}
	}
}

func (d *Daemon) doSendReponseToRemote(req *pb.PCRequest) *pb.PCResponse {
	callID, err := uuid.FromBytes(req.CallId)
	if err != nil {
		return errorUnaryCallString(
			callID,
			"malformed request: call id not in UUID format",
		)
	}

	rc, found := d.responseWaiters.Load(callID)
	if !found {
		return errorUnaryCallString(
			callID,
			fmt.Sprintf("Response for call id %d not requested or cancelled", callID),
		)
	}

	rc.(chan *pb.PCRequest) <- req

	return okUnaryCallResponse(callID)
}

type safeWriter struct {
	w ggio.Writer
	m sync.Mutex
}

func (sw *safeWriter) WriteMsg(msg proto.Message) error {
	sw.m.Lock()
	defer sw.m.Unlock()
	return sw.w.WriteMsg(msg)
}

func errorUnaryCall(callID uuid.UUID, err error) *pb.PCResponse {
	message := err.Error()
	return &pb.PCResponse{
		CallId: callID[:],
		Message: &pb.PCResponse_DaemonError{
			DaemonError: &pb.DaemonError{Message: &message},
		},
	}
}

func errorUnaryCallString(callID uuid.UUID, errMsg string) *pb.PCResponse {
	return &pb.PCResponse{
		CallId: callID[:],
		Message: &pb.PCResponse_DaemonError{
			DaemonError: &pb.DaemonError{Message: &errMsg},
		},
	}
}

func okUnaryCallResponse(callID uuid.UUID) *pb.PCResponse {
	return &pb.PCResponse{CallId: callID[:]}
}

func okCancelled(callID uuid.UUID) *pb.PCResponse {
	return &pb.PCResponse{
		CallId: callID[:],
		Message: &pb.PCResponse_Cancel{
			Cancel: &pb.Cancel{},
		},
	}
}
