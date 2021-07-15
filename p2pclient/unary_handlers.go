package p2pclient

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-daemon/pb"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type UnaryHandler func([]byte) ([]byte, error)

func (c *Client) ensurePersistentConn() error {
	if c.persistentConn == nil {
		conn, err := c.getPersistentConn()
		if err != nil {
			return err
		}

		c.persistentConn = conn
	}

	return nil
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

	// upgrade control connection to a persistent one
	if err := c.persistentConn.WriteRequest(
		&pb.Request{
			Type: pb.Request_PERSISTENT_CONN_UPGRADE.Enum(),
		},
	); err != nil {
		return nil, err
	}

	return c.persistentConn, nil
}

func (c *Client) NewUnaryHandler(proto string, handler UnaryHandler) error {
	control, err := c.getPersistentConn()
	if err != nil {
		return err
	}

	req := &pb.Request{
		Type: pb.Request_ADD_UNARY_HANDLER.Enum(),
		AddUnaryHandler: &pb.AddUnaryHandlerRequest{
			Proto: &proto,
		},
	}
	if err := control.WriteRequest(req); err != nil {
		return err
	}

	go detachHandler(
		control,
		protocol.ID(proto),
		handler,
	)

	// TODO: get status

	return nil
}

func detachHandler(c MultiplexedConn, proto protocol.ID, handler UnaryHandler) {
	for {
		req, err := c.ReadUnaryRequest(proto)
		if err != nil {
			log.Debugw("failed to read request", err)
			return
		}

		go func() {
			callID := *req.RequestHandling.CallId
			result, err := handler(req.RequestHandling.Data)
			if err != nil {
				c.WriteRequest(errorUnaryCallResponse(callID, err))
				return
			}

			c.WriteRequest(
				&pb.Request{
					Type: pb.Request_SEND_RESPONSE_TO_REMOTE.Enum(),
					SendResponseToRemote: &pb.SendResponseToRemote{
						CallId: req.RequestHandling.CallId,
						Data:   result,
					},
				},
			)
		}()
	}
}

func (c *Client) UnaryCall(p peer.ID, proto protocol.ID, data []byte) ([]byte, error) {
	control, err := c.getPersistentConn()
	if err != nil {
		return nil, err
	}

	callID := rand.Int63()
	timeout := int64(1)

	req := &pb.Request{
		Type: pb.Request_CALL_UNARY.Enum(),
		CallUnary: &pb.CallUnaryRequest{
			Peer:   []byte(p),
			Proto:  (*string)(&proto),
			Data:   data,
			CallId: &callID,
			// TODO: client option
			Timeout: &timeout,
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
		Type: pb.Request_SEND_RESPONSE_TO_REMOTE.Enum(),
		SendResponseToRemote: &pb.SendResponseToRemote{
			CallId: &callID,
			Error:  &errMsg,
		},
	}
}
