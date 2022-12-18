package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/astrviktor/otus-databases-project/internal/storage"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/config"
	_ "github.com/go-sql-driver/mysql" //nolint
	"github.com/google/uuid"
)

type Storage struct {
	dsn                  string
	dbMaxConnectAttempts int
	mutex                *sync.RWMutex
	db                   *sql.DB
	segments             []uuid.UUID
}

func New(config config.DBConfig) *Storage {
	mutex := sync.RWMutex{}

	return &Storage{
		dsn:                  config.DSN,
		dbMaxConnectAttempts: config.MaxConnectAttempts,
		mutex:                &mutex,
		db:                   nil,
		segments:             nil,
	}
}

func (s *Storage) CreateConnect() error {
	db, err := sql.Open("mysql", s.dsn)
	if err != nil {
		return err
	}

	for i := 0; i < s.dbMaxConnectAttempts; i++ {
		log.Println("trying to connect to MySQL...")
		err = db.Ping()
		if err == nil {
			break
		}
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		return err
	}

	db.SetMaxOpenConns(100)
	s.db = db

	log.Println("connect to MySQL status OK")

	return nil
}

func (s *Storage) CloseConnect() {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			log.Printf("failed to close db: %s", err)
		}
	}
}

func (s *Storage) CreateClients(size int) error {
	const batchSize = 10000
	clients := make([]storage.Client, batchSize, batchSize)

	var msisdn uint64 = 79000000000
	var gender = [3]rune{'M', 'F', ' '}

	log.Println("Creating Clients in Clickhouse, size: ", size)
	start := time.Now()

	counter := 0

	for i := 0; i < size; i++ {
		client := storage.Client{}

		msisdn++
		counter++

		client.Msisdn = msisdn
		client.Gender = gender[rand.Intn(3)]
		client.Age = uint8(rand.Intn(83) + 18)
		client.Income = float32(rand.Intn(9000000)/100 + 10000)
		client.Counter = 0

		clients[counter-1] = client

		if counter == batchSize {
			log.Println("batching 1 ...")
			err := s.CreateClientsBatch(clients)
			if err != nil {
				return err
			}
			counter = 0
		}
	}

	if counter > 0 {
		log.Println("batching 2 ...")
		err := s.CreateClientsBatch(clients[:counter])
		if err != nil {
			return err
		}
	}

	log.Printf("Creating Clients in Clickhouse, time: %v \n", time.Since(start))

	return nil

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
	//return nil

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
	//
	//return nil
}

func (s *Storage) DeleteClients() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	query := `CALL delete_clients();`

	_, err = tx.Exec(query)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) CreateSegment(size int) (uuid.UUID, error) {
	id := uuid.New()
	s.mutex.Lock()
	s.segments = append(s.segments, id)
	s.mutex.Unlock()

	//idWithoutHyphens := strings.Replace(id.String(), "-", "", -1)

	start := time.Now()

	//tx, err := s.db.Begin()
	//if err != nil {
	//	log.Println("ERROR segment 1: ", err.Error())
	//	return id, err
	//}
	//
	//viewQuery := `CREATE MATERIALIZED VIEW creator.` + idWithoutHyphens + ` ENGINE = Memory POPULATE
	//AS SELECT msisdn
	//   FROM creator.clients
	//   ORDER BY counter
	//   LIMIT ?;`
	//
	//_, err = tx.Exec(viewQuery, size)
	//if err != nil {
	//	log.Println("ERROR segment 2: ", err.Error())
	//	return id, err
	//}
	//
	//err = tx.Commit()
	//if err != nil {
	//	log.Println("ERROR segment 3: ", err.Error())
	//	return id, err
	//}

	t1 := time.Since(start)
	start = time.Now()

	tx, err := s.db.Begin()
	if err != nil {
		log.Println("ERROR segment 1: ", err.Error())
		return id, err
	}

	segmentQuery := `INSERT INTO creator.segments(id, msisdn)
	SELECT UUID_TO_BIN(?),msisdn
	   FROM creator.clients
       ORDER BY counter
  	   LIMIT ?;`

	_, err = tx.Exec(segmentQuery, id, size)
	if err != nil {
		log.Println("ERROR segment 4: ", err.Error())
		return id, err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("ERROR segment 5: ", err.Error())
		return id, err
	}

	t2 := time.Since(start)
	start = time.Now()

	tx, err = s.db.Begin()
	if err != nil {
		log.Println("ERROR segment 1: ", err.Error())
		return id, err
	}

	counterQuery := `UPDATE creator.clients
	SET counter = counter + 1
	WHERE msisdn in (select msisdn from creator.segments where id = UUID_TO_BIN(?));`

	//creator.` + idWithoutHyphens + `);`

	_, err = tx.Exec(counterQuery, id)
	if err != nil {
		log.Println("ERROR segment 6: ", err.Error())
		return id, err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("ERROR segment 7: ", err.Error())
		return id, err
	}

	t3 := time.Since(start)
	start = time.Now()

	//tx, err = s.db.Begin()
	//if err != nil {
	//	log.Println("ERROR segment 1: ", err.Error())
	//	return id, err
	//}
	//
	//dropQuery := `DROP TABLE creator.` + idWithoutHyphens + `;`
	//
	//_, err = tx.Exec(dropQuery)
	//if err != nil {
	//	log.Println("ERROR segment 8: ", err.Error())
	//	return id, err
	//}
	//
	//err = tx.Commit()
	//if err != nil {
	//	log.Println("ERROR segment 9: ", err.Error())
	//	return id, err
	//}

	t4 := time.Since(start)

	log.Println("Clickhouse times:", t1.Milliseconds(), t2.Milliseconds(),
		t3.Milliseconds(), t4.Milliseconds())
	return id, nil

	//id := uuid.New()
	//
	//tx, err := s.db.Begin()
	//if err != nil {
	//	log.Println("ERROR: MySQL MaxOpenConnections:", s.db.Stats().InUse)
	//
	//	return id, err
	//}
	//
	//query := `CALL create_segment(UUID_TO_BIN(?), ?);`
	//
	//_, err = tx.Exec(query, id, size)
	//if err != nil {
	//	return id, err
	//}
	//
	//err = tx.Commit()
	//if err != nil {
	//	return id, err
	//}
	//
	//return id, nil

}

func (s *Storage) CreateClientsBatch(clients []storage.Client) error {
	log.Println("CreateClientsBatch ... size:", len(clients))

	valueStrings := []string{}
	valueArgs := []interface{}{}
	for _, client := range clients {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")

		valueArgs = append(valueArgs, client.Msisdn)
		valueArgs = append(valueArgs, string(client.Gender))
		valueArgs = append(valueArgs, client.Age)
		valueArgs = append(valueArgs, client.Income)
		valueArgs = append(valueArgs, client.Counter)
	}

	query := "INSERT INTO creator.clients (msisdn, gender, age, income, counter) VALUES %s"

	query = fmt.Sprintf(query, strings.Join(valueStrings, ","))
	//fmt.Println("query:", query)

	tx, err := s.db.Begin()
	if err != nil {
		log.Println("ERROR 1: ", err.Error())
		return err
	}

	_, err = tx.Exec(query, valueArgs...)
	if err != nil {
		log.Println("ERROR 2: ", err.Error())
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Println("ERROR 3: ", err.Error())
		return err
	}

	return nil
}

func (s *Storage) GetSegment() (uuid.UUID, int, error) {
	s.mutex.RLock()
	count := len(s.segments)
	n := rand.Intn(count)
	id := s.segments[n]
	s.mutex.RUnlock()

	msisdns := make([]storage.Msisdn, 0)

	query := `SELECT msisdn
	FROM creator.segments 
	WHERE id = UUID_TO_BIN(?);`

	rows, err := s.db.Query(query, id)
	if err != nil {
		return id, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var msisdn uint64
		err = rows.Scan(&msisdn)

		if errors.Is(err, sql.ErrNoRows) {
			break
		}

		if err != nil {
			return id, 0, err
		}

		if rows.Err() != nil {
			return id, 0, err
		}
		msisdns = append(msisdns, storage.Msisdn{Msisdn: msisdn})
	}

	return id, len(msisdns), nil
}
