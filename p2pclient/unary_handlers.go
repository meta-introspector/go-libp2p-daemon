package p2pclient

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-daemon/pb"
)

var defaultTimeout = int64(1)

type UnaryHandler func([]byte) ([]byte, error)

func (uh UnaryHandler) handle(c MultiplexedConn, callID int64, req *pb.Response) {
	result, err := uh(req.RequestHandling.Data)
	if err != nil {
		c.WriteRequest(errorUnaryCallResponse(callID, err))
		return
	}

	c.WriteRequest(
		&pb.Request{
			Type:   pb.Request_SEND_RESPONSE_TO_REMOTE.Enum(),
			CallId: &callID,
			SendResponseToRemote: &pb.SendResponseToRemote{
				Data: result,
			},
		},
	)
}

func (c *Client) getPersistentConn() (MultiplexedConn, error) {
	if c.persistentConn != nil {
		return c.persistentConn, nil
	}

	control, err := c.newControlConn()
	if err != nil {
		return nil, err
	}

	c.persistentConn = NewMultiplexedConn(
		control,
		network.MessageSizeMax,
	)

	if err := c.persistentConn.WriteRequest(
		&pb.Request{
			Type: pb.Request_PERSISTENT_CONN_UPGRADE.Enum(),
		},
	); err != nil {
		return nil, err
	}

	return c.persistentConn, nil
}

func (c *Client) NewUnaryHandler(proto protocol.ID, handler UnaryHandler) error {
	control, err := c.getPersistentConn()
	if err != nil {
		return err
	}

	callID := NewCallID()
	req := &pb.Request{
		Type: pb.Request_ADD_UNARY_HANDLER.Enum(),
		AddUnaryHandler: &pb.AddUnaryHandlerRequest{
			Proto: (*string)(&proto),
		},
		CallId: (*int64)(&callID),
	}
	if err := control.WriteRequest(req); err != nil {
		return err
	}

	go listenProtoRequests(
		control,
		protocol.ID(proto),
		handler,
	)

	return nil
}

func listenProtoRequests(c MultiplexedConn, proto protocol.ID, handler UnaryHandler) {
	for {
		req, err := c.ReadUnaryRequest(proto)
		if err != nil {
			log.Debugw("failed to read request", err)
			return
		}

		callID := *req.CallId
		go handler.handle(c, callID, req)
	}
}

func (c *Client) UnaryCall(p peer.ID, proto protocol.ID, data []byte) ([]byte, error) {
	control, err := c.getPersistentConn()
	if err != nil {
		return nil, err
	}

	callID := NewCallID()
	req := &pb.Request{
		Type:   pb.Request_CALL_UNARY.Enum(),
		CallId: &callID,
		CallUnary: &pb.CallUnaryRequest{
			Peer:    []byte(p),
			Proto:   (*string)(&proto),
			Data:    data,
			Timeout: &defaultTimeout,
		},
	}

	if err := control.WriteRequest(req); err != nil {
		return nil, err
	}
	resp, err := c.persistentConn.ReadUnaryResponse(callID)
	if err != nil {
		return nil, err
	}

	if resp.CallUnaryResponse.Error != nil {
		return nil, NewRemoteError(*resp.CallUnaryResponse.Error)
	}

	if pb.Response_ERROR == resp.GetType() {
		errMsg := *resp.Error.Msg
		return nil, fmt.Errorf(errMsg)
	}

	return resp.CallUnaryResponse.Result, nil
}

func NewRemoteError(message string) *RemoteError {
	return &RemoteError{message}
}

// RemoteError is returned when remote peer failed to handle a request
type RemoteError struct {
	msg string
}

func (re *RemoteError) Error() string {
	return fmt.Sprintf("remote peer failed to handle request: %s", re.msg)
}

func errorUnaryCallResponse(callID int64, err error) *pb.Request {
	errMsg := err.Error()
	return &pb.Request{
		Type:   pb.Request_SEND_RESPONSE_TO_REMOTE.Enum(),
		CallId: &callID,
		SendResponseToRemote: &pb.SendResponseToRemote{
			Data:  make([]byte, 0),
			Error: &errMsg,
		},
	}
}
