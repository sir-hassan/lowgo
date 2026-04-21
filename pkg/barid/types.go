package barid

import "errors"

var (
	ErrNilPipeChannel    = errors.New("barid: nil pipe channel")
	ErrDuplicatePipeCode = errors.New("barid: duplicate pipe code")
)

// Pipe describes one endpoint in the router.
//
// Code identifies the source endpoint. The router does not use this value when
// dispatching messages, but requiring an explicit code keeps endpoint
// registration symmetrical with drains and helps callers model their topology.
type Pipe struct {
	Code    byte
	Channel chan Frame
}

// Frame is one routed message.
type Frame struct {
	Code  byte
	Bytes []byte
}
