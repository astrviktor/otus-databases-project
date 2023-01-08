package tarantool

import (
	"github.com/astrviktor/otus-databases-project/internal/storage"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/config"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool"
)

type Storage struct {
	description          string
	dsn                  string
	dbMaxConnectAttempts int
	mutex                *sync.RWMutex
	conn                 *tarantool.Connection
	segments             []uuid.UUID
}

func New(config config.DBConfig) *Storage {
	mutex := sync.RWMutex{}

	return &Storage{
		description:          "Tarantool",
		dsn:                  config.DSN,
		dbMaxConnectAttempts: config.MaxConnectAttempts,
		mutex:                &mutex,
		conn:                 nil,
		segments:             nil,
	}
}

func (s *Storage) GetDescription() string {
	return s.description
}

func (s *Storage) CreateConnect() error {
	opts := tarantool.Opts{Timeout: 1 * time.Second,
		Reconnect:     1 * time.Second,
		MaxReconnects: uint(s.dbMaxConnectAttempts),
		Concurrency:   100,
		User:          "user", Pass: "password"}

	conn, err := tarantool.Connect(s.dsn, opts)
	if err != nil {
		log.Println(s.dsn)
		return err
	}

	s.conn = conn

	log.Println("connect to Tarantool status OK")

	err = s.Init()
	if err != nil {
		return err
	}

	log.Println("init Tarantool tables status OK")

	err = s.conn.Close()
	if err != nil {
		return err
	}

	conn, err = tarantool.Connect(s.dsn, opts)
	if err != nil {
		return err
	}

	s.conn = conn
	log.Println("reconnect to Tarantool status OK")

	return nil
}

func (s *Storage) CloseConnect() {
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			log.Printf("failed to close Tarantool: %s", err)
		}
	}
}

func (s *Storage) Init() error {
	//box.schema.space.create('clients', {if_not_exists = true})

	_, err := s.conn.Call("box.schema.space.create", []interface{}{
		"clients", map[string]bool{"if_not_exists": true}})

	if err != nil {
		log.Println(err)
		//return err
	}

	//box.space.clients:format({
	//	{name = 'msisdn', type = 'unsigned'},
	//	{name = 'gender', type = 'string'},
	//	{name = 'age', type = 'unsigned'},
	//	{name = 'income', type = 'double'},
	//	{name = 'nextuse', type = 'unsigned'}
	//})

	_, err = s.conn.Call("box.space.clients:format", [][]map[string]string{
		{
			{"name": "msisdn", "type": "unsigned"},
			{"name": "gender", "type": "string"},
			{"name": "age", "type": "unsigned"},
			{"name": "income", "type": "double"},
			{"name": "nextuse", "type": "unsigned"},
		}})

	if err != nil {
		log.Println(2)
		return err
	}

	//box.space.clients:create_index('primary', {
	//	type = 'hash',
	//	unique = true,
	//	parts = {'msisdn'}
	//})

	_, err = s.conn.Call("box.space.clients:create_index", []interface{}{
		"primary",
		map[string]interface{}{
			"type":          "hash",
			"unique":        true,
			"parts":         []string{"msisdn"},
			"if_not_exists": true}})

	if err != nil {
		log.Println(3)
		return err
	}

	//box.space.clients:create_index('nextuse', {
	//	type = 'tree',
	//	unique = false,
	//	parts = {'nextuse'}
	//})

	_, err = s.conn.Call("box.space.clients:create_index", []interface{}{
		"nextuse",
		map[string]interface{}{
			"type":          "tree",
			"unique":        false,
			"parts":         []string{"nextuse"},
			"if_not_exists": true}})

	if err != nil {
		log.Println(4)
		return err
	}

	////////////////////////////////////////////////////////////
	//box.schema.space.create('segments', {if_not_exists = true})

	_, err = s.conn.Call("box.schema.space.create", []interface{}{
		"segments", map[string]bool{"if_not_exists": true}})

	if err != nil {
		log.Println(err)
		//return err
	}

	//box.space.segments:format({
	//	{name = 'id', type = 'uuid'},
	//	{name = 'msisdn', type = 'unsigned'}
	//})

	_, err = s.conn.Call("box.space.segments:format", [][]map[string]string{
		{
			{"name": "id", "type": "uuid"},
			{"name": "msisdn", "type": "unsigned"},
		}})

	if err != nil {
		log.Println(5)
		return err
	}

	//box.space.segments:create_index('primary', {unique = true, parts = {
	//	{field = 'id', type = 'uuid'},
	//	{field = 'msisdn', type = 'unsigned'}
	//}})

	_, err = s.conn.Call("box.space.segments:create_index", []interface{}{
		"primary",
		map[string]interface{}{
			"unique":        true,
			"parts":         []string{"id", "msisdn"},
			"if_not_exists": true}})

	if err != nil {
		log.Println(6)
		return err
	}

	return nil
}

func (s *Storage) CreateClients(size int) error {
	const batchSize = 10000
	clients := make([]storage.ClientTarantool, batchSize, batchSize)

	var msisdn uint64 = 79000000000
	var gender = [3]rune{'M', 'F', ' '}

	log.Println("Creating Clients in Tarantool, size: ", size)
	start := time.Now()

	counter := 0
	date := time.Now().UTC().AddDate(0, 0, -10) //.Format("2006-01-02")
	date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())

	date.Unix()

	for i := 0; i < size; i++ {
		client := storage.ClientTarantool{}

		msisdn++
		counter++

		client.Msisdn = msisdn
		client.Gender = string(gender[rand.Intn(3)])
		client.Age = uint64(rand.Intn(83) + 18)
		client.Income = float64(rand.Intn(9000000)/100 + 10000)
		client.NextUse = date.Unix()

		clients[counter-1] = client

		if counter == batchSize {

			for idx := 0; idx < counter; idx++ {
				_, err := s.conn.Insert("clients", []interface{}{
					clients[idx].Msisdn,
					clients[idx].Gender,
					clients[idx].Age,
					clients[idx].Income,
					clients[idx].NextUse,
				})

				if err != nil {
					return err
				}
			}
			counter = 0
		}
	}

	if counter > 0 {
		for idx := 0; idx < counter; idx++ {
			_, err := s.conn.Insert("clients", []interface{}{
				clients[idx].Msisdn,
				clients[idx].Gender,
				clients[idx].Age,
				clients[idx].Income,
				clients[idx].NextUse,
			})

			if err != nil {
				return err
			}
		}
	}

	log.Printf("creating DB in Tarantool, time: %v \n", time.Since(start))

	return nil
}

func (s *Storage) DeleteClients() error {
	//box.space.clients:truncate()

	_, err := s.conn.Call("box.space.clients:truncate", []interface{}{})

	return err
}

func (s *Storage) CreateSegment(size int) (uuid.UUID, error) {
	id := uuid.New()
	s.mutex.Lock()
	s.segments = append(s.segments, id)
	s.mutex.Unlock()

	//date := time.Now().UTC()
	//date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
	//
	//clientsCollection := s.client.Database("creator").Collection("clients")
	//
	////filter := bson.D{{}, bson.D{{"_id", 1}}}
	//filter := bson.M{"nextuse": bson.M{"$lt": date}}
	////opts := options.Find().SetSort(bson.D{{"rating", -1}}).SetLimit(2).SetSkip(1)
	//
	////opts := options.Find().SetSort(bson.D{{"counter", 1}}).SetLimit(int64(size))
	//opts := options.Find().SetProjection(bson.D{{"_id", 1}}).SetLimit(int64(size))
	//
	//clientsCursor, err := clientsCollection.Find(context.TODO(), filter, opts)
	//if err != nil {
	//	log.Println("ERROR 1: ", err.Error())
	//	return id, err
	//}
	//
	//var msisdns []storage.Msisdn
	////var results []bson.M
	////var clients []storage.Client
	//if err = clientsCursor.All(context.TODO(), &msisdns); err != nil {
	//	log.Println("ERROR 2: ", err.Error())
	//	return id, err
	//}
	//
	//size = len(msisdns)
	////log.Println("msisdns: ", msisdns)
	////segment := make([]interface{}, size, size)
	////
	////for i := 0; i < len(clients); i++ {
	////	segmentItem := storage.SegmentItem{
	////		Id:     id.String(),
	////		Msisdn: clients[i].Msisdn,
	////	}
	////	segment[i] = segmentItem
	////}
	//
	////segment := make([]interface{}, size, size)
	////
	////for i := 0; i < len(msisdns); i++ {
	////	segmentItem := bson.D{
	////		{"id", id.String()}, {"msisdn", msisdns[i]},
	////	}
	////	segment[i] = segmentItem
	////}
	//
	//segment := make([]interface{}, size, size)
	//numbers := make([]int64, size, size)
	//
	//for i := 0; i < len(msisdns); i++ {
	//	segmentItem := storage.SegmentItem{
	//		Id:     id.String(),
	//		Msisdn: msisdns[i].Msisdn,
	//	}
	//	segment[i] = segmentItem
	//	numbers[i] = int64(msisdns[i].Msisdn)
	//}
	//
	//result, err := clientsCollection.UpdateMany(
	//	context.TODO(),
	//	//bson.M{},
	//	//bson.M{"title": "The Polyglot Developer Podcast"},
	//	bson.M{
	//		"_id": bson.M{"$in": numbers},
	//	},
	//	bson.D{
	//		{"$set", bson.D{{"nextuse", date}}},
	//	},
	//)
	//log.Printf("Updated %v Documents!\n", result.ModifiedCount)
	//
	//segmentsCollection := s.client.Database("creator").Collection("segments")
	//_, err = segmentsCollection.InsertMany(context.TODO(), segment)
	//if err != nil {
	//	log.Println("ERROR 3: ", err.Error())
	//	return id, err
	//}
	//
	////db.getSiblingDB("creator").getCollection("segments").insertOne(
	////  { _id: "12345678-1234-5678-1234-567812345678", msisdns: [79852003798, 79852003799, 79852003797]}
	////)
	//
	////segmentsCollection := s.client.Database("creator").Collection("segments")
	////
	////segment := bson.D{{"_id", id.String()}, {"msisdns", msisdns}}
	////_, err = segmentsCollection.InsertOne(context.TODO(), segment)
	////if err != nil {
	////	return id, err
	////}
	//
	////db.getSiblingDB("creator").getCollection("segments").insertMany(
	////  { id: "12345678-1234-5678-1234-567812345678", msisdn: 79852003798},
	////  { id: "12345678-1234-5678-1234-567812345678", msisdn: 79852003799}
	//// , 79852003799, 79852003797]}
	////)
	//
	////type segmentElem struct {
	////	id     uuid.UUID
	////	msisdn uint64
	////}
	////
	////segment := make([]segmentElem, 0, size)
	////
	////for i := 0; i < len(clients); i++ {
	////	msisdn := segmentElem{id, clients[i].Msisdn}
	////
	////	segment = append(segment, msisdn)
	////}

	return id, nil
}

func (s *Storage) GetSegment() (uuid.UUID, int, error) {

	s.mutex.RLock()
	count := len(s.segments)
	n := rand.Intn(count)
	id := s.segments[n]
	s.mutex.RUnlock()

	msisdns := make([]storage.Msisdn, 0)
	//
	//segmentsCollection := s.client.Database("creator").Collection("segments")
	//
	////filter := bson.D{{}, bson.D{{"_id", 1}}}
	//filter := bson.D{{"id", id.String()}}
	////opts := options.Find().SetSort(bson.D{{"rating", -1}}).SetLimit(2).SetSkip(1)
	//opts := options.Find().SetProjection(bson.D{{"msisdn", 1}, {"_id", 0}})
	//
	//clientsCursor, err := segmentsCollection.Find(context.TODO(), filter, opts)
	//if err != nil {
	//	return id, 0, err
	//}
	//
	////msisdns := make([]uint64, size, size)
	////var results []bson.M
	//msisdns := make([]storage.Msisdn, 0)
	//if err = clientsCursor.All(context.TODO(), &msisdns); err != nil {
	//	return id, 0, err
	//}

	return id, len(msisdns), nil
}
