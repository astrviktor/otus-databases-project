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
	Msisdn  uint64  `json:"msisdn" bson:"_id"`
	Gender  rune    `json:"gender" bson:"gender"`
	Age     uint8   `json:"age" bson:"age"`
	Income  float32 `json:"income" bson:"income"`
	Counter uint32  `json:"counter" bson:"counter"`
}

type Msisdn struct {
	Msisdn uint64 `json:"msisdn"`
}
