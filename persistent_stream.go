package p2pd

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	ggio "github.com/gogo/protobuf/io"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-daemon/pb"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (d *Daemon) handleUpgradedConn(r ggio.Reader, w ggio.Writer) {
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

	pid, err := peer.IDFromBytes(req.CallUnary.Peer)
	if err != nil {
		return errorResponseString(
			fmt.Sprintf("Failed to parse peer id: %v", err),
		)
	}

	protos := make([]protocol.ID, len(req.CallUnary.Proto))
	for x, str := range req.CallUnary.Proto {
		protos[x] = protocol.ID(str)
	}

	ctx, cancel := d.requestContext(req.CallUnary.GetTimeout())
	defer cancel()

	// TODO: cache streams
	s, err := d.host.NewStream(ctx, pid, protos...)
	if err != nil {
		return errorResponse(err)
	}
	defer s.Close()

	requestData := req.CallUnary.GetData()
	if _, err := s.Write(requestData); err != nil {
		return errorResponseString(
			fmt.Sprintf("Failed to write message: %s", err.Error()),
		)
	}

	resp := okResponse()
	resp.CallUnaryResponse = &pb.CallUnaryResponse{}
	if _, err := s.Read(resp.CallUnaryResponse.Result); err != nil {
		return errorResponseString(
			fmt.Sprintf("Failed to read message: %s", err.Error()),
		)
	}

	return resp
}

func (d *Daemon) doAddUnaryHandler(w ggio.Writer, req *pb.Request) *pb.Response {
	if req.AddUnaryHandler == nil {
		return malformedRequestErrorResponse()
	}

	d.mx.Lock()
	defer d.mx.Unlock()

	for _, sp := range req.AddUnaryHandler.Proto {
		p := protocol.ID(sp)
		if _, registered := d.registeredUnaryProtocols[p]; !registered {
			d.host.SetStreamHandler(p, d.getPersistentStreamHandler(w))
		}
		log.Debugw("set unary stream handler", "protocol", sp)
	}

	return okResponse()
}

func (d *Daemon) getPersistentStreamHandler(clientWriter ggio.Writer) network.StreamHandler {
	return func(s network.Stream) {
		msg := makeStreamInfo(s)
		err := clientWriter.WriteMsg(msg)
		if err != nil {
			log.Debugw("error accepting stream", "error", err)
			s.Reset()
			return
		}

		// get request data from remote peer
		var data []byte
		if _, err := s.Read(data); err != nil {
			log.Debugw("error reading request data", "error", err)
			return
		}

		// now we need to request the client to handle this data
		resp := okResponse()
		callId := rand.Int63()
		resp.RequestHandling = &pb.RequestHandling{
			CallId: &callId,
			Data:   data,
		}
		clientWriter.WriteMsg(resp)

		responseWaiter := make(chan *pb.Request)

		d.responseWaiters.Store(
			callId,
			responseWaiter,
		)

		// and wait for it to return the results for us to write
		// it to the remote peer

		response := <-responseWaiter
		if _, err := s.Write(response.SendResponseToRemote.Data); err != nil {
			log.Debugw("failed to write response to remote", "error", err)
			return
		}
	}
}

func (d *Daemon) doSendReponseToRemote(req *pb.Request) *pb.Response {
	if req.SendResponseToRemote == nil {
		return malformedRequestErrorResponse()
	}
	callId := *req.SendResponseToRemote

	responseC, found := d.responseWaiters.LoadAndDelete(callId)
	if !found {
		return errorResponseString(
			fmt.Sprintf("Response for call id %d not requested", *callId.CallId),
		)
	}

	responseChan := responseC.(chan *pb.Request)
	responseChan <- req

	return nil
}
