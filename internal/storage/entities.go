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
	Msisdn uint64  `json:"msisdn" bson:"_id,omitempty"`
	Gender rune    `json:"gender" bson:"gender,omitempty"`
	Age    int     `json:"age" bson:"age,omitempty"`
	Income float64 `json:"income" bson:"income,omitempty"`
	Next   uint64  `json:"next" bson:"next,omitempty"`
}

type Msisdn struct {
	Msisdn uint64 `json:"msisdn"`
}
