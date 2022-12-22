package mongodb

import (
	"github.com/astrviktor/otus-databases-project/internal/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/config"

	"context"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Storage struct {
	description          string
	dsn                  string
	dbMaxConnectAttempts int
	mutex                *sync.RWMutex
	client               *mongo.Client
	segments             []uuid.UUID
}

func New(config config.DBConfig) *Storage {
	mutex := sync.RWMutex{}

	return &Storage{
		description:          "MongoDB",
		dsn:                  config.DSN,
		dbMaxConnectAttempts: config.MaxConnectAttempts,
		mutex:                &mutex,
		client:               nil,
		segments:             nil,
	}
}

func (s *Storage) GetDescription() string {
	return s.description
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
	if s.client != nil {
		if err := s.client.Disconnect(ctx); err != nil {
			log.Printf("failed to close MongoDB: %s", err)
		}
	}
}

func (s *Storage) CreateClients(size int) error {
	const batchSize = 10000
	clients := make([]interface{}, batchSize, batchSize)

	clientsCollection := s.client.Database("creator").Collection("clients")

	var msisdn uint64 = 79000000000
	var gender = [3]rune{'M', 'F', ' '}

	log.Println("Creating Clients in MongoDB, size: ", size)
	start := time.Now()

	counter := 0
	date := time.Now().UTC().AddDate(0, 0, -10) //.Format("2006-01-02")
	date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())

	for i := 0; i < size; i++ {
		client := storage.ClientMongo{}

		msisdn++
		counter++

		client.Msisdn = msisdn
		client.Gender = gender[rand.Intn(3)]
		client.Age = uint8(rand.Intn(83) + 18)
		client.Income = float32(rand.Intn(9000000)/100 + 10000)
		client.NextUse = date

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

	indexModel := mongo.IndexModel{Keys: bson.D{{"nextuse", 1}}}
	_, err := clientsCollection.Indexes().CreateOne(context.TODO(), indexModel)
	if err != nil {
		return err
	}

	indexModel = mongo.IndexModel{Keys: bson.D{{"id", 1}}}
	segmentsCollection := s.client.Database("creator").Collection("segments")
	_, err = segmentsCollection.Indexes().CreateOne(context.TODO(), indexModel)
	if err != nil {
		return err
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

	return nil
}

func (s *Storage) CreateSegment(size int) (uuid.UUID, error) {
	//db.getSiblingDB("creator").getCollection("clients").find({},{_id: 1}).limit(size)

	id := uuid.New()
	s.mutex.Lock()
	s.segments = append(s.segments, id)
	s.mutex.Unlock()

	date := time.Now().UTC()
	date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())

	clientsCollection := s.client.Database("creator").Collection("clients")

	//filter := bson.D{{}, bson.D{{"_id", 1}}}
	filter := bson.M{"nextuse": bson.M{"$lt": date}}
	//opts := options.Find().SetSort(bson.D{{"rating", -1}}).SetLimit(2).SetSkip(1)

	//opts := options.Find().SetSort(bson.D{{"counter", 1}}).SetLimit(int64(size))
	opts := options.Find().SetProjection(bson.D{{"_id", 1}}).SetLimit(int64(size))

	clientsCursor, err := clientsCollection.Find(context.TODO(), filter, opts)
	if err != nil {
		log.Println("ERROR 1: ", err.Error())
		return id, err
	}

	var msisdns []storage.Msisdn
	//var results []bson.M
	//var clients []storage.Client
	if err = clientsCursor.All(context.TODO(), &msisdns); err != nil {
		log.Println("ERROR 2: ", err.Error())
		return id, err
	}

	size = len(msisdns)
	//log.Println("msisdns: ", msisdns)
	//segment := make([]interface{}, size, size)
	//
	//for i := 0; i < len(clients); i++ {
	//	segmentItem := storage.SegmentItem{
	//		Id:     id.String(),
	//		Msisdn: clients[i].Msisdn,
	//	}
	//	segment[i] = segmentItem
	//}

	//segment := make([]interface{}, size, size)
	//
	//for i := 0; i < len(msisdns); i++ {
	//	segmentItem := bson.D{
	//		{"id", id.String()}, {"msisdn", msisdns[i]},
	//	}
	//	segment[i] = segmentItem
	//}

	segment := make([]interface{}, size, size)
	numbers := make([]int64, size, size)

	for i := 0; i < len(msisdns); i++ {
		segmentItem := storage.SegmentItem{
			Id:     id.String(),
			Msisdn: msisdns[i].Msisdn,
		}
		segment[i] = segmentItem
		numbers[i] = int64(msisdns[i].Msisdn)
	}

	result, err := clientsCollection.UpdateMany(
		context.TODO(),
		//bson.M{},
		//bson.M{"title": "The Polyglot Developer Podcast"},
		bson.M{
			"_id": bson.M{"$in": numbers},
		},
		bson.D{
			{"$set", bson.D{{"nextuse", date}}},
		},
	)
	log.Printf("Updated %v Documents!\n", result.ModifiedCount)

	segmentsCollection := s.client.Database("creator").Collection("segments")
	_, err = segmentsCollection.InsertMany(context.TODO(), segment)
	if err != nil {
		log.Println("ERROR 3: ", err.Error())
		return id, err
	}

	//db.getSiblingDB("creator").getCollection("segments").insertOne(
	//  { _id: "12345678-1234-5678-1234-567812345678", msisdns: [79852003798, 79852003799, 79852003797]}
	//)

	//segmentsCollection := s.client.Database("creator").Collection("segments")
	//
	//segment := bson.D{{"_id", id.String()}, {"msisdns", msisdns}}
	//_, err = segmentsCollection.InsertOne(context.TODO(), segment)
	//if err != nil {
	//	return id, err
	//}

	//db.getSiblingDB("creator").getCollection("segments").insertMany(
	//  { id: "12345678-1234-5678-1234-567812345678", msisdn: 79852003798},
	//  { id: "12345678-1234-5678-1234-567812345678", msisdn: 79852003799}
	// , 79852003799, 79852003797]}
	//)

	//type segmentElem struct {
	//	id     uuid.UUID
	//	msisdn uint64
	//}
	//
	//segment := make([]segmentElem, 0, size)
	//
	//for i := 0; i < len(clients); i++ {
	//	msisdn := segmentElem{id, clients[i].Msisdn}
	//
	//	segment = append(segment, msisdn)
	//}

	return id, nil
}

func (s *Storage) GetSegment() (uuid.UUID, int, error) {
	//db.getSiblingDB("creator").getCollection("segments").find({id: "12345678-1234-5678-1234-567812345678"},{msisdn:1, _id:0})

	s.mutex.RLock()
	count := len(s.segments)
	n := rand.Intn(count)
	id := s.segments[n]
	s.mutex.RUnlock()

	//msisdns := make([]storage.Msisdn, 0)

	segmentsCollection := s.client.Database("creator").Collection("segments")

	//filter := bson.D{{}, bson.D{{"_id", 1}}}
	filter := bson.D{{"id", id.String()}}
	//opts := options.Find().SetSort(bson.D{{"rating", -1}}).SetLimit(2).SetSkip(1)
	opts := options.Find().SetProjection(bson.D{{"msisdn", 1}, {"_id", 0}})

	clientsCursor, err := segmentsCollection.Find(context.TODO(), filter, opts)
	if err != nil {
		return id, 0, err
	}

	//msisdns := make([]uint64, size, size)
	//var results []bson.M
	msisdns := make([]storage.Msisdn, 0)
	if err = clientsCursor.All(context.TODO(), &msisdns); err != nil {
		return id, 0, err
	}

	return id, len(msisdns), nil
}
