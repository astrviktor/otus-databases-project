package aerospike

import (
	"errors"
	"fmt"
	"github.com/astrviktor/otus-databases-project/internal/storage"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/config"

	"github.com/aerospike/aerospike-client-go/v6"
	"github.com/google/uuid"
)

type Storage struct {
	description          string
	host                 string
	port                 int
	dbMaxConnectAttempts int
	mutex                *sync.RWMutex
	client               *aerospike.Client
	namespace            string
	segments             []uuid.UUID
}

func New(config config.DBConfig) *Storage {
	mutex := sync.RWMutex{}

	return &Storage{
		description:          "Aerospike",
		host:                 config.Host,
		port:                 config.Port,
		dbMaxConnectAttempts: config.MaxConnectAttempts,
		mutex:                &mutex,
		client:               nil,
		namespace:            "creator",
		segments:             nil,
	}
}

func (s *Storage) GetDescription() string {
	return s.description
}

func (s *Storage) CreateConnect() error {

	client, err := aerospike.NewClient(s.host, s.port)
	if err != nil {
		log.Println("Error to connect to Aerospike", s.host, s.port)
		return err
	}

	if client.IsConnected() {
		fmt.Println("Connect to Aerospike OK")
	}

	s.client = client
	return nil
}

func (s *Storage) CloseConnect() {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *Storage) CreateClientsIndexes() error {
	// CREATE INDEX clients_age_idx ON creator.clients (age) NUMERIC

	idxTask, err := s.client.CreateIndex(nil, s.namespace,
		"clients", "clients_nextuse_idx", "nextuse", aerospike.NUMERIC)
	if err != nil {
		log.Println("Error create Aerospike index")
		return err
	}

	// wait until index is created.
	// OnComplete() channel will return nil on success and an error on errors
	err = <-idxTask.OnComplete()
	if err != nil {
		log.Println("Error Aerospike index task")
		return err
	}

	return nil
}

func (s *Storage) CreateSegmentsIndexes() error {
	// CREATE INDEX segments_uuid_idx ON creator.segments (uuid) STRING

	idxTask, err := s.client.CreateIndex(nil, s.namespace,
		"segments", "segments_uuid_idx", "uuid", aerospike.STRING)
	if err != nil {
		log.Println("Error create Aerospike index")
		return err
	}

	// wait until index is created.
	// OnComplete() channel will return nil on success and an error on errors
	err = <-idxTask.OnComplete()
	if err != nil {
		log.Println("Error Aerospike index task")
		return err
	}

	return nil
}

func (s *Storage) PutBatchClients(clients []storage.ClientAerospike) error {
	// Define Operation Expressions
	//exp := aerospike.ExpMapPut(aerospike.DefaultMapPolicy(), aerospike.ExpStringVal("recent"),
	//	aerospike.ExpAnd(
	//		aerospike.ExpGreater(
	//			aerospike.ExpIntBin("occurred"),
	//			aerospike.ExpIntVal(20211231)),
	//		aerospike.ExpBinExists("posted")),
	//	aerospike.ExpMapBin("report"))
	//
	//// Create batch of records
	//batchRecords := []aerospike.BatchRecordIfc{}
	//
	//for i := 0; i < len(clients); i++ {
	//	// Create key
	//	key, err := aerospike.NewKey("creator", "clients", clients[i].Msisdn)
	//	if err != nil {
	//		fmt.Println("Aerospike key error")
	//		return err
	//	}
	//
	//	// Create record
	//	record := aerospike.NewBatchWrite(nil, key,
	//		aerospike.ExpWriteOp("report", exp, aerospike.ExpWriteFlagDefault),
	//		aerospike.GetBinOp("report"))
	//	// Add to batch
	//	batchRecords = append(batchRecords, record)
	//}
	//
	//
	//err := s.client.BatchOperate(batchPolicy, batchRecords)
	//
	//key, err := aerospike.NewKey(s.namespace, "clients", client.Msisdn)
	//
	//if err != nil {
	//	fmt.Println("Aerospike key error")
	//	return err
	//}
	//
	//record := aerospike.BinMap{
	//	//"msisdn":  client.Msisdn,
	//	"gender":  client.Gender,
	//	"age":     client.Age,
	//	"income":  client.Income,
	//	"nextuse": client.NextUse,
	//}
	//
	//writePolicy := aerospike.NewWritePolicy(0, 0)
	//writePolicy.SendKey = true
	//
	//err = s.client.Put(writePolicy, key, record)
	//if err != nil {
	//	fmt.Println("Aerospike write error")
	//	return err
	//}

	return nil
}

func (s *Storage) PutOneClient(client storage.ClientAerospike) error {
	key, err := aerospike.NewKey(s.namespace, "clients", client.Msisdn)

	if err != nil {
		fmt.Println("Aerospike key error")
		return err
	}

	record := aerospike.BinMap{
		//"msisdn":  client.Msisdn,
		"gender":  client.Gender,
		"age":     client.Age,
		"income":  client.Income,
		"nextuse": client.NextUse,
	}

	writePolicy := aerospike.NewWritePolicy(0, 0)
	writePolicy.SendKey = true

	err = s.client.Put(writePolicy, key, record)
	if err != nil {
		fmt.Println("Aerospike write error")
		return err
	}

	return nil
}

func (s *Storage) CreateClients(size int) error {
	const batchSize = 10000
	clients := make([]storage.ClientAerospike, batchSize, batchSize)

	var msisdn int64 = 79000000000
	var gender = [3]rune{'M', 'F', ' '}

	log.Println("Creating Clients in Aerospike, size: ", size)
	start := time.Now()

	counter := 0
	date := time.Now().UTC().AddDate(0, 0, -10) //.Format("2006-01-02")
	date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())

	date.Unix()

	for i := 0; i < size; i++ {
		client := storage.ClientAerospike{}

		msisdn++
		counter++

		client.Msisdn = msisdn
		client.Gender = string(gender[rand.Intn(3)])
		client.Age = int64(rand.Intn(83) + 18)
		client.Income = float64(rand.Intn(9000000)/100 + 10000)
		client.NextUse = date.Unix()

		clients[counter-1] = client

		if counter == batchSize {

			for idx := 0; idx < counter; idx++ {
				err := s.PutOneClient(clients[idx])
				if err != nil {
					return err
				}
			}
			counter = 0
		}
	}

	if counter > 0 {
		for idx := 0; idx < counter; idx++ {
			err := s.PutOneClient(clients[idx])
			if err != nil {
				return err
			}
		}
	}

	err := s.CreateClientsIndexes()
	if err != nil {
		return err
	}

	err = s.CreateSegmentsIndexes()
	if err != nil {
		return err
	}

	log.Printf("creating Clients in Aerospike, time: %v \n", time.Since(start))

	return nil
}

func (s *Storage) DeleteClients() error {
	return s.client.Truncate(nil, s.namespace, "clients", nil)
}

func (s *Storage) CreateSegment(size int) (uuid.UUID, error) {
	start := time.Now()

	id := uuid.New()
	s.mutex.Lock()
	s.segments = append(s.segments, id)
	s.mutex.Unlock()

	date := time.Now().UTC()
	date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
	//date.AddDate(10, 0, 0).Unix()

	// find clients

	stm := aerospike.NewStatement(s.namespace, "clients")

	//log.Println("filter date start: ", date.Unix())
	//log.Println("filter date end: ", date.AddDate(10, 0, 0).Unix())

	err := stm.SetFilter(aerospike.NewRangeFilter("nextuse",
		0, date.Unix()))
	if err != nil {
		log.Println("ERROR set filter: ", err.Error())
		return id, err
	}

	recordset, err := s.client.Query(nil, stm)
	if err != nil {
		log.Println("ERROR query: ", err.Error())
		return id, err
	}

	var msisdns []int64

	// consume recordset and check errors
	for {
		result, ok := <-recordset.Results()

		if !ok {
			break
		}

		if result.Err != nil {
			log.Println("ERROR record read: ", err.Error())
			return id, result.Err
		}

		msisdn, ok := result.Record.Key.Value().GetObject().(int64)
		if !ok {
			return id, errors.New("error type casting for key to int64")
		}

		// process record
		msisdns = append(msisdns, msisdn)
	}

	// consume recordset and check errors
	//for i := 0; i < size; i++ {
	//	record, err := recordset.Read()
	//
	//	if err != nil {
	//		log.Println("ERROR record read: ", err.Error())
	//		return id, err
	//	}
	//
	//	msisdn, ok := record.Key.Value().GetObject().(int64)
	//	if !ok {
	//		return id, errors.New("error type casting for key to int64")
	//	}
	//
	//	// process record
	//	msisdns = append(msisdns, msisdn)
	//}

	//log.Println("ERROR segment 6: ", err.Error())
	log.Println("find clients done: ", time.Since(start).Milliseconds())

	// add clients to segments
	// key = "segment_id:msisdn" value = msisdn
	// add index

	for i := 0; i < size; i++ {
		PK := id.String() + ":" + strconv.FormatInt(msisdns[i], 10)

		key, err := aerospike.NewKey(s.namespace, "segments", PK)
		if err != nil {
			return id, err
		}

		bins := aerospike.BinMap{
			"uuid":   id.String(),
			"msisdn": msisdns[i],
		}

		// write policy
		writePolicy := aerospike.NewWritePolicy(0, 0)
		writePolicy.SendKey = true

		// write the bins
		err = s.client.Put(writePolicy, key, bins)
		if err != nil {
			return id, err
		}
	}
	log.Println("create segment done: ", time.Since(start).Milliseconds())

	// create index?
	//err = s.CreateSegmentsIndexes()
	//if err != nil {
	//	return id, err
	//}
	//log.Println("create index done: ", time.Since(start).Milliseconds())

	// update clients
	next := date.AddDate(0, 0, 10)

	for i := 0; i < size; i++ {
		key, err := aerospike.NewKey(s.namespace, "clients", msisdns[i])
		if err != nil {
			return id, err
		}

		rec, err := s.client.Get(nil, key)
		if err != nil {
			return id, err
		}

		// write policy
		writePolicy := aerospike.NewWritePolicy(0, 0)
		writePolicy.SendKey = true

		rec.Bins["nextuse"] = next.Unix()

		err = s.client.Put(writePolicy, key, rec.Bins)

		if err != nil {
			return id, err
		}
	}

	log.Println("update clients done: ", time.Since(start).Milliseconds())
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

	var msisdns []int

	// find segment

	stm := aerospike.NewStatement(s.namespace, "segments")

	//log.Println("filter date start: ", date.Unix())
	//log.Println("filter date end: ", date.AddDate(10, 0, 0).Unix())

	err := stm.SetFilter(aerospike.NewEqualFilter("uuid", id.String()))
	if err != nil {
		log.Println("ERROR set filter: ", err.Error())
		return id, 0, err
	}

	recordset, err := s.client.Query(nil, stm)
	if err != nil {
		log.Println("ERROR query: ", err.Error())
		return id, 0, err
	}

	// consume recordset and check errors
	for {
		result, ok := <-recordset.Results()

		if !ok {
			break
		}

		if result.Err != nil {
			log.Println("ERROR record read: ", err.Error())
			return id, 0, result.Err
		}

		// process record
		msisdn, ok := result.Record.Bins["msisdn"].(int)
		if !ok {
			return id, 0, errors.New("error type casting for key to int64")
		}

		log.Println(msisdn)

		msisdns = append(msisdns, msisdn)
	}

	// consume recordset and check errors
	//for {
	//	record, err := recordset.Read()
	//
	//	if err != nil {
	//		break
	//	}
	//
	//	// process record
	//	msisdn, ok := record.Bins["msisdn"].(int)
	//	if !ok {
	//		return id, 0, errors.New("error type casting for key to int64")
	//	}
	//
	//	//log.Println(msisdn)
	//
	//	msisdns = append(msisdns, msisdn)
	//}

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
