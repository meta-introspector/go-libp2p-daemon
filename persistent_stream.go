package p2pd

import (
	"encoding/json"
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

func protoprint(m proto.Message) {
	bytes, err := json.Marshal(m)
	fmt.Println("received from client:", string(bytes), err)
}

func (d *Daemon) handleUpgradedConn(r ggio.Reader, unsafeW ggio.Writer) {
	w := &safeWriter{w: unsafeW}

	for {
		var req pb.PCRequest
		if err := r.ReadMsg(&req); err != nil && err != io.EOF {
			log.Debugw("error reading message", "error", err)
			return
		}

		protoprint(&req)
		callID := req.CallId

		switch req.Message.(type) {
		case *pb.PCRequest_AddUnaryHandler:
			resp := d.doAddUnaryHandler(w, callID, req.GetAddUnaryHandler())
			if err := w.WriteMsg(resp); err != nil {
				log.Debugw("error reading message", "error", err)
				return
			}

		case *pb.PCRequest_CallUnary:
			resp := d.doUnaryCall(callID, req.GetCallUnary())
			if err := w.WriteMsg(resp); err != nil {
				log.Debugw("error reading message", "error", err)
				return
			}

		case *pb.PCRequest_UnaryResponse:
			resp := d.doSendReponseToRemote(&req)
			if err := w.WriteMsg(resp); err != nil {
				log.Debugw("error reading message", "error", err)
				return
			}
		}
	}
}
func (d *Daemon) doUnaryCall(callID []byte, req *pb.CallUnaryRequest) *pb.PCResponse {
	pid, err := peer.IDFromBytes(req.Peer)
	if err != nil {
		return errorUnaryCall(callID, err)
	}

	ctx, cancel := d.requestContext(req.GetTimeout())
	defer cancel()

	remoteStream, err := d.host.NewStream(
		ctx,
		pid,
		protocol.ID(*req.Proto),
	)
	if err != nil {
		return errorUnaryCall(callID, err)
	}
	defer remoteStream.Close()

	if err := ggio.NewDelimitedWriter(remoteStream).
		WriteMsg(req); err != nil {
		return errorUnaryCall(callID, err)
	}

	fmt.Println("waddup?")
	remoteResp := &pb.PCRequest{}
	if err := ggio.NewDelimitedReader(remoteStream, network.MessageSizeMax).ReadMsg(remoteResp); err != nil {
		fmt.Println("FAILED HERE")
		return errorUnaryCall(callID, err)
	}

	fmt.Println("waddup?")
	resp := okUnaryCallResponse(callID)
	resp.Message = &pb.PCResponse_CallUnaryResponse{
		CallUnaryResponse: remoteResp.GetUnaryResponse(),
	}

	return resp
}

func (d *Daemon) doAddUnaryHandler(w ggio.Writer, callID []byte, req *pb.AddUnaryHandlerRequest) *pb.PCResponse {
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

// getPersistentStreamHandler returns a lib-p2p stream handler tied to a
// given persistent client stream
func (d *Daemon) getPersistentStreamHandler(cw ggio.Writer) network.StreamHandler {
	return func(s network.Stream) {
		defer s.Close()

		// read request from remote peer
		req := &pb.PCRequest{}
		if err := ggio.NewDelimitedReader(s, network.MessageSizeMax).ReadMsg(req); err != nil {
			log.Debugw("failed to read proto from incoming p2p stream", err)
			return
		}

		resp := &pb.PCResponse{
			CallId: req.CallId,
			Message: &pb.PCResponse_RequestHandling{
				RequestHandling: req.GetCallUnary(),
			},
		}
		if err := cw.WriteMsg(resp); err != nil {
			log.Debugw("failed to write message to client", err)
			return
		}

		callID, err := uuid.FromBytes(req.CallId)
		if err != nil {
			cw.WriteMsg(
				errorUnaryCallString(
					req.CallId,
					"malformed request: call id not in UUID format",
				),
			)
		}

		rWaiter := make(chan *pb.PCRequest)
		d.responseWaiters.Store(
			callID,
			rWaiter,
		)

		response := <-rWaiter
		if err := ggio.NewDelimitedWriter(s).WriteMsg(response); err != nil {
			fmt.Println("shit shit shti sthi")
			log.Debugw("failed to write to p2p stream: ", err)
			return
		}
	}
}

func (d *Daemon) doSendReponseToRemote(req *pb.PCRequest) *pb.PCResponse {
	callID, err := uuid.FromBytes(req.CallId)
	if err != nil {
		return errorUnaryCallString(
			req.CallId,
			"malformed request: call id not in UUID format",
		)
	}

	responseC, found := d.responseWaiters.LoadAndDelete(callID)
	if !found {
		return errorUnaryCallString(
			req.CallId,
			fmt.Sprintf("Response for call id %d not requested", callID),
		)
	}

	responseChan := responseC.(chan *pb.PCRequest)
	responseChan <- req

	return okUnaryCallResponse(req.CallId)
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

func errorUnaryCall(callID []byte, err error) *pb.PCResponse {
	message := err.Error()
	return &pb.PCResponse{
		CallId: callID,
		Message: &pb.PCResponse_DaemonError{
			DaemonError: &pb.DaemonError{Message: &message},
		},
	}
}

func errorUnaryCallString(callID []byte, errMsg string) *pb.PCResponse {
	return &pb.PCResponse{
		CallId: callID,
		Message: &pb.PCResponse_DaemonError{
			DaemonError: &pb.DaemonError{Message: &errMsg},
		},
	}
}

func okUnaryCallResponse(callID []byte) *pb.PCResponse {
	return &pb.PCResponse{CallId: callID}
}
