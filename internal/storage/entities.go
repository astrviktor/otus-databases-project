package storage

import (
	"github.com/google/uuid"
)

type Storage interface {
	CreateConnect() error
	CloseConnect()
	CreateClients(size int) error
	DeleteClients() error
	CreateSegment(size int) (uuid.UUID, error)
}

type Client struct {
	Msisdn uint64  `json:"msisdn"`
	Gender rune    `json:"gender"`
	Age    int     `json:"age"`
	Income float64 `json:"income"`
}

type Msisdn struct {
	Msisdn uint64 `json:"msisdn"`
}
