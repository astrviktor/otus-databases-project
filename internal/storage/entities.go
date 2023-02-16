package storage

import (
	"github.com/google/uuid"
	"time"
)

type Storage interface {
	GetDescription() string
	CreateConnect() error
	CloseConnect()
	CreateClients(size int) error
	DeleteClients() error
	CreateSegment(size int) (uuid.UUID, error)
	GetSegment() (uuid.UUID, int, error)
}

type Client struct {
	Msisdn  uint64  `json:"msisdn" bson:"_id"`
	Gender  rune    `json:"gender" bson:"gender"`
	Age     uint8   `json:"age" bson:"age"`
	Income  float32 `json:"income" bson:"income"`
	NextUse string  `json:"nextuse" bson:"nextuse"`
}

type ClientMongo struct {
	Msisdn  uint64    `json:"msisdn" bson:"_id"`
	Gender  rune      `json:"gender" bson:"gender"`
	Age     uint8     `json:"age" bson:"age"`
	Income  float32   `json:"income" bson:"income"`
	NextUse time.Time `json:"nextuse" bson:"nextuse"`
}

type ClientTarantool struct {
	Msisdn  uint64  `json:"msisdn"`
	Gender  string  `json:"gender"`
	Age     uint64  `json:"age"`
	Income  float64 `json:"income"`
	NextUse int64   `json:"nextuse"`
}

type ClientAerospike struct {
	Msisdn  int64   `json:"msisdn"`
	Gender  string  `json:"gender"`
	Age     int64   `json:"age"`
	Income  float64 `json:"income"`
	NextUse int64   `json:"nextuse"`
}

type GeoObject struct {
	Id          string     `json:"id"`
	Coordinates [2]float64 `json:"coordinates"`
	Comment     string     `json:"comment"`
}

type Msisdn struct {
	Msisdn uint64 `json:"msisdn" bson:"_id"`
}

type SegmentItem struct {
	Id     string `json:"id" bson:"id"`
	Msisdn uint64 `json:"msisdn" bson:"msisdn"`
}
