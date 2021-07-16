package p2pclient

import (
	"math/rand"
	"time"
)

// initialize random seed for CallID generation
func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewCallID creates a new CallID
func NewCallID() int64 {
	return rand.Int63()
}
