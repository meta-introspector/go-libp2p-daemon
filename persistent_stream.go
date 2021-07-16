package p2pd

import (
	"fmt"
	"io"
	"sync"

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
		var req pb.Request
		if err := r.ReadMsg(&req); err != nil && err != io.EOF {
			log.Debugw("error reading message", "error", err)
			return
		}
		log.Debugw("request", "type", req.GetType())

		switch req.GetType() {
		case pb.Request_CALL_UNARY:
			resp := d.doUnaryCall(&req)
			if err := w.WriteMsg(resp); err != nil {
				log.Debugw("error reading message", "error", err)
				return
			}

		case pb.Request_ADD_UNARY_HANDLER:
			resp := d.doAddUnaryHandler(w, &req)
			if err := w.WriteMsg(resp); err != nil {
				log.Debugw("error reading message", "error", err)
				return
			}

		case pb.Request_SEND_RESPONSE_TO_REMOTE:
			resp := d.doSendReponseToRemote(&req)
			if err := w.WriteMsg(resp); err != nil {
				log.Debugw("error reading message", "error", err)
				return
			}
		}
	}
}
func (d *Daemon) doUnaryCall(req *pb.Request) *pb.Response {
	if req.CallUnary == nil {
		return malformedRequestErrorResponse()
	}

	callID := *req.CallUnary.CallId

	pid, err := peer.IDFromBytes(req.CallUnary.Peer)
	if err != nil {
		return errorUnaryCall(callID, err)
	}

	ctx, cancel := d.requestContext(req.CallUnary.GetTimeout())
	defer cancel()

	remoteStream, err := d.host.NewStream(
		ctx,
		pid,
		protocol.ID(*req.CallUnary.Proto),
	)
	if err != nil {
		return errorUnaryCall(callID, err)
	}
	defer remoteStream.Close()

	if err := ggio.NewDelimitedWriter(remoteStream).
		WriteMsg(req); err != nil {
		return errorUnaryCall(callID, err)
	}

	remoteResp := &pb.Request{}
	if err := ggio.NewDelimitedReader(remoteStream, network.MessageSizeMax).
		ReadMsg(remoteResp); err != nil {
		return errorUnaryCall(callID, err)
	}

	resp := okResponse()
	// resp.CallId = remoteResp.CallId
	resp.CallUnaryResponse = remoteResp.SendResponseToRemote

	return resp
}

func (d *Daemon) doAddUnaryHandler(w ggio.Writer, req *pb.Request) *pb.Response {
	if req.AddUnaryHandler == nil {
		return malformedRequestErrorResponse()
	}

	d.mx.Lock()
	defer d.mx.Unlock()

	p := protocol.ID(*req.AddUnaryHandler.Proto)
	if _, registered := d.registeredUnaryProtocols[p]; !registered {
		d.host.SetStreamHandler(p, d.getPersistentStreamHandler(w))
	}
	log.Debugw("set unary stream handler", "protocol", p)

	return okResponse()
}

// getPersistentStreamHandler returns a lib-p2p stream handler tied to a
// given persistent client stream
func (d *Daemon) getPersistentStreamHandler(cw ggio.Writer) network.StreamHandler {
	return func(s network.Stream) {
		defer s.Close()

		req := &pb.Request{}
		if err := ggio.NewDelimitedReader(s, network.MessageSizeMax).
			ReadMsg(req); err != nil {
			log.Debugw("failed to read proto from incoming p2p stream", err)
			return
		} else if req.CallUnary == nil {
			log.Debugw("bad handler request: call data not specified")
			return
		}

		resp := okResponse()
		// resp.CallId = req.CallId
		resp.RequestHandling = req.CallUnary

		if err := cw.WriteMsg(resp); err != nil {
			log.Debugw("failed to write message to client", err)
			return
		}

		rWaiter := make(chan *pb.Request)
		d.responseWaiters.Store(
			*req.CallUnary.CallId,
			rWaiter,
		)

		response := <-rWaiter
		if err := ggio.NewDelimitedWriter(s).WriteMsg(response); err != nil {
			log.Debugw("failed to write to p2p stream: ", err)
			return
		}
	}
}

func (d *Daemon) doSendReponseToRemote(req *pb.Request) *pb.Response {
	if req.SendResponseToRemote == nil {
		return malformedRequestErrorResponse()
	}
	callID := *req.SendResponseToRemote.CallId

	responseC, found := d.responseWaiters.LoadAndDelete(callID)
	if !found {
		return errorResponseString(
			fmt.Sprintf("Response for call id %d not requested", callID),
		)
	}

	responseChan := responseC.(chan *pb.Request)
	responseChan <- req

	return okResponse()
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
