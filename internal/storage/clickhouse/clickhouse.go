package clickhouse

import (
	"database/sql"
	"github.com/astrviktor/otus-databases-project/internal/storage"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/astrviktor/otus-databases-project/internal/config"
	//_ "github.com/jackc/pgx/stdlib" //nolint
)

type Storage struct {
	dsn                  string
	dbMaxConnectAttempts int
	db                   *sql.DB
}

func New(config config.DBConfig) *Storage {
	return &Storage{
		dsn:                  config.DSN,
		dbMaxConnectAttempts: config.MaxConnectAttempts,
		db:                   nil,
	}
}

func (s *Storage) CreateConnect() error {

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{s.dsn},
		Auth: clickhouse.Auth{
			Database: "creator",
			Username: "default",
			Password: "",
		},
		//TLS: &tls.Config{
		//	InsecureSkipVerify: true,
		//},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			clickhouse.CompressionLZ4, 0,
		},
		Debug:           false,
		BlockBufferSize: 10,
	})
	//db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)

	//if err != nil {
	//	return err
	//}

	var err error
	for i := 0; i < s.dbMaxConnectAttempts; i++ {
		log.Println("trying to connect to Clickhouse...")
		err = db.Ping()
		if err == nil {
			break
		}
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		return err
	}

	s.db = db

	log.Println("connect to Clickhouse status OK")

	return nil
}

func (s *Storage) CloseConnect() {
	if err := s.db.Close(); err != nil {
		log.Printf("failed to close clickhouse db: %s", err)
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

	//return nil

	//tx, err := s.db.Begin()
	//if err != nil {
	//	return err
	//}
	//
	//query := `CALL creator.create_clients($1);`
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
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	query := `ALTER TABLE creator.clients DELETE WHERE 1=1;`

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

//func (s *Storage) CreateSegment(size int) (uuid.UUID, error) {
//	id := uuid.New()
//	idWithoutHyphens := strings.Replace(id.String(), "-", "", -1)
//
//	start := time.Now()
//
//	tx, err := s.db.Begin()
//	if err != nil {
//		log.Println("ERROR segment 1: ", err.Error())
//		return id, err
//	}
//
//	viewQuery := `CREATE MATERIALIZED VIEW creator.` + idWithoutHyphens + ` ENGINE = Memory POPULATE
//	AS SELECT msisdn
//	FROM creator.clients
//	WHERE msisdn NOT IN (
//		SELECT msisdn FROM creator.counters
//		GROUP BY msisdn
//		HAVING sum(counter) > (select min(sum_counter) from sum_msisdn_counter) + 1)
//		LIMIT ?;`
//
//	_, err = tx.Exec(viewQuery, size)
//	if err != nil {
//		log.Println("ERROR segment 2: ", err.Error())
//		return id, err
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		log.Println("ERROR segment 3: ", err.Error())
//		return id, err
//	}
//
//	t1 := time.Since(start)
//	start = time.Now()
//
//	tx, err = s.db.Begin()
//	if err != nil {
//		log.Println("ERROR segment 1: ", err.Error())
//		return id, err
//	}
//
//	segmentQuery := `INSERT INTO creator.segments(id, msisdn)
//	SELECT ?,msisdn
//	FROM creator.` + idWithoutHyphens + `;`
//
//	_, err = tx.Exec(segmentQuery, id)
//	if err != nil {
//		log.Println("ERROR segment 4: ", err.Error())
//		return id, err
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		log.Println("ERROR segment 5: ", err.Error())
//		return id, err
//	}
//
//	t2 := time.Since(start)
//	start = time.Now()
//
//	tx, err = s.db.Begin()
//	if err != nil {
//		log.Println("ERROR segment 1: ", err.Error())
//		return id, err
//	}
//
//	counterQuery := `INSERT INTO creator.counters(msisdn, counter)
//	SELECT msisdn, 1
//	FROM creator.` + idWithoutHyphens + `;`
//
//	_, err = tx.Exec(counterQuery)
//	if err != nil {
//		log.Println("ERROR segment 6: ", err.Error())
//		return id, err
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		log.Println("ERROR segment 7: ", err.Error())
//		return id, err
//	}
//
//	t3 := time.Since(start)
//	start = time.Now()
//
//	tx, err = s.db.Begin()
//	if err != nil {
//		log.Println("ERROR segment 1: ", err.Error())
//		return id, err
//	}
//
//	dropQuery := `DROP TABLE creator.` + idWithoutHyphens + `;`
//
//	_, err = tx.Exec(dropQuery)
//	if err != nil {
//		log.Println("ERROR segment 8: ", err.Error())
//		return id, err
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		log.Println("ERROR segment 9: ", err.Error())
//		return id, err
//	}
//
//	t4 := time.Since(start)
//
//	log.Println("Clickhouse times:", t1.Milliseconds(), t2.Milliseconds(),
//		t3.Milliseconds(), t4.Milliseconds())
//	return id, nil
//
//}

func (s *Storage) CreateSegment(size int) (uuid.UUID, error) {
	id := uuid.New()
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
	SELECT ?,msisdn
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

	counterQuery := `ALTER TABLE creator.clients
	UPDATE counter = counter + 1
	WHERE msisdn in (select msisdn from creator.segments where id = ?);`

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

}

func (s *Storage) CreateClient(client storage.Client) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	query := `INSERT INTO creator.clients
    (msisdn, gender, age, income, counter)
	VALUES ($1, $2, $3, $4, $5);`

	_, err = tx.Exec(query, client.Msisdn, client.Gender, client.Age, client.Income, client.Counter)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) CreateClientsBatch(clients []storage.Client) error {
	log.Println("CreateClientsBatch ... size:", len(clients))
	tx, err := s.db.Begin()
	if err != nil {
		log.Println("ERROR 1: ", err.Error())
		return err
	}

	batch, err := tx.Prepare("INSERT INTO creator.clients")
	if err != nil {
		log.Println("ERROR 2: ", err.Error())
		return err
	}

	//query := `INSERT INTO creator.clients
	//(msisdn, gender, age, income, counter)
	//VALUES (?, ?, ?, ?, ?);`

	//batch, err := tx.Prepare(query)

	for i := 0; i < len(clients); i++ {
		_, err := batch.Exec(
			clients[i].Msisdn,
			string(clients[i].Gender),
			clients[i].Age,
			clients[i].Income,
			clients[i].Counter,
		)
		if err != nil {
			log.Println("ERROR 3: ", err.Error())
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Println("ERROR 4: ", err.Error())
		return err
	}

	return nil
}
