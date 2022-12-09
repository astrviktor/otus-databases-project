package mongodb

import (
	"github.com/astrviktor/otus-databases-project/internal/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"math/rand"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/config"

	"context"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Storage struct {
	dsn                  string
	dbMaxConnectAttempts int
	client               *mongo.Client
}

func New(config config.DBConfig) *Storage {
	return &Storage{
		dsn:                  config.DSN,
		dbMaxConnectAttempts: config.MaxConnectAttempts,
		client:               nil,
	}
}

func (s *Storage) CreateConnect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(s.dsn))
	if err != nil {
		log.Println(s.dsn)
		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i := 0; i < s.dbMaxConnectAttempts; i++ {
		log.Println("trying to connect to MongoDB...")
		err = client.Ping(ctx, readpref.Primary())
		if err == nil {
			break
		}
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		return err
	}

	//db.SetMaxOpenConns(100)
	s.client = client

	log.Println("connect to MongoDB status OK")

	return nil
}

func (s *Storage) CloseConnect() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.client.Disconnect(ctx); err != nil {
		log.Printf("failed to close MongoDB: %s", err)
	}
}

func (s *Storage) CreateClients(size int) error {
	const batchSize = 10000
	clients := make([]interface{}, batchSize, batchSize)

	clientsCollection := s.client.Database("creator").Collection("clients")

	var msisdn uint64 = 79000000000
	var gender = [3]rune{'M', 'F', ' '}

	log.Println("creating DB in MongoDB, size: ", size)
	start := time.Now()

	counter := 0

	for i := 0; i < size; i++ {
		client := storage.Client{}

		msisdn++
		counter++

		client.Msisdn = msisdn
		client.Gender = gender[rand.Intn(3)]
		client.Age = rand.Intn(83) + 18
		client.Income = rand.Float64()*90000 + 10000
		client.Next = msisdn + 1

		clients[counter-1] = client

		if counter == batchSize {
			_, err := clientsCollection.InsertMany(context.TODO(), clients)
			if err != nil {
				return err
			}
			counter = 0
		}

		//_, err := clientsCollection.InsertOne(context.TODO(), client)

	}

	if counter > 0 {
		_, err := clientsCollection.InsertMany(context.TODO(), clients[:counter])
		if err != nil {
			return err
		}
	}

	log.Printf("creating DB in MongoDB, time: %v \n", time.Since(start))

	//clients := []interface{}{
	//	bson.D{{"_id", 79852003798}, {"gender", "M"}, {"age", 20},
	//		{"income", 10000.00}, {"next", 79852003799}},
	//	bson.D{{"_id", 79852003799}, {"gender", "M"}, {"age", 21},
	//		{"income", 10000.00}, {"next", 79852003800}},
	//	bson.D{{"_id", 79852003800}, {"gender", "M"}, {"age", 22},
	//		{"income", 10000.00}, {"next", 79852003801}},
	//}
	//
	//_, err := clientsCollection.InsertMany(context.TODO(), clients)
	//// check for errors in the insertion
	//if err != nil {
	//	return err
	//}
	//tx, err := s.db.Begin()
	//if err != nil {
	//	return err
	//}
	//
	//query := `CALL create_clients(?);`
	//
	//_, err = tx.Exec(query, size)
	//if err != nil {
	//	return err
	//}
	//
	//err = tx.Commit()
	//if err != nil {
	//	return err
	//}

	return nil
}

func (s *Storage) DeleteClients() error {
	clientsCollection := s.client.Database("creator").Collection("clients")

	if err := clientsCollection.Drop(context.TODO()); err != nil {
		return err
	}
	//tx, err := s.db.Begin()
	//if err != nil {
	//	return err
	//}
	//
	//query := `CALL delete_clients();`
	//
	//_, err = tx.Exec(query)
	//if err != nil {
	//	return err
	//}
	//
	//err = tx.Commit()
	//if err != nil {
	//	return err
	//}

	return nil
}

func (s *Storage) CreateSegment(size int) (uuid.UUID, error) {
	//db.getSiblingDB("creator").getCollection("clients").find({},{_id: 1}).limit(size)

	id := uuid.New()

	clientsCollection := s.client.Database("creator").Collection("clients")

	//filter := bson.D{{}, bson.D{{"_id", 1}}}
	filter := bson.D{}
	//opts := options.Find().SetSort(bson.D{{"rating", -1}}).SetLimit(2).SetSkip(1)
	opts := options.Find().SetLimit(int64(size))

	clientsCursor, err := clientsCollection.Find(context.TODO(), filter, opts)
	if err != nil {
		return id, err
	}

	//msisdns := make([]uint64, size, size)
	//var results []bson.M
	var clients []storage.Client
	if err = clientsCursor.All(context.TODO(), &clients); err != nil {
		return id, err
	}

	msisdns := make([]uint64, 0, size)
	//log.Println("count of results:", len(clients))
	for i := 0; i < len(clients); i++ {
		msisdns = append(msisdns, clients[i].Msisdn)
		//log.Println(clients[i])
	}
	//db.getSiblingDB("creator").getCollection("segments").insertOne(
	//  { _id: "12345678-1234-5678-1234-567812345678", msisdns: [79852003798, 79852003799, 79852003797]}
	//)

	segmentsCollection := s.client.Database("creator").Collection("segments")

	segment := bson.D{{"_id", id.String()}, {"msisdns", msisdns}}
	_, err = segmentsCollection.InsertOne(context.TODO(), segment)
	if err != nil {
		return id, err
	}

	return uuid.New(), nil
}
