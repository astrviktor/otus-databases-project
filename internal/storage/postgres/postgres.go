package postgres

import (
	"context"
	"github.com/astrviktor/otus-databases-project/internal/storage"
	"github.com/jackc/pgx/v5"
	"log"
	"math/rand"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/config"
	"github.com/google/uuid"
	//_ "github.com/jackc/pgx/stdlib" //nolint
	_ "github.com/jackc/pgx/v5" //nolint
)

type Storage struct {
	dsn                  string
	dbMaxConnectAttempts int
	//db                   *sql.DB
	db *pgx.Conn
}

func New(config config.DBConfig) *Storage {
	return &Storage{
		dsn:                  config.DSN,
		dbMaxConnectAttempts: config.MaxConnectAttempts,
		db:                   nil,
	}
}

func (s *Storage) CreateConnect() error {
	//db, err := sql.Open("pgx", s.dsn)
	db, err := pgx.Connect(context.Background(), s.dsn) //os.Getenv("DATABASE_URL"))

	if err != nil {
		log.Println("ERROR connect 1: ", err.Error())
		return err
	}

	for i := 0; i < s.dbMaxConnectAttempts; i++ {
		log.Println("trying to connect to Postgres...")
		err = db.Ping(context.Background())
		if err == nil {
			break
		}
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		log.Println("ERROR connect 2: ", err.Error())
		return err
	}

	//db.SetMaxOpenConns(100)
	s.db = db

	log.Println("connect to Postgres status OK")

	return nil
}

func (s *Storage) CloseConnect() {
	if err := s.db.Close(context.Background()); err != nil {
		log.Printf("failed to close db: %s", err)
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
	//
	return nil
}

func (s *Storage) DeleteClients() error {
	tx, err := s.db.Begin(context.Background())
	if err != nil {
		return err
	}

	query := `CALL creator.delete_clients();`

	_, err = tx.Exec(context.Background(), query)
	if err != nil {
		return err
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}

	return nil
}

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

	tx, err := s.db.Begin(context.Background())
	if err != nil {
		log.Println("ERROR segment 1: ", err.Error())
		return id, err
	}

	segmentQuery := `INSERT INTO creator.segments(id, msisdn)
	SELECT $1,msisdn
	   FROM creator.clients
       ORDER BY counter
  	   LIMIT $2;`

	_, err = tx.Exec(context.Background(), segmentQuery, id, size)
	if err != nil {
		log.Println("ERROR segment 4: ", err.Error())
		return id, err
	}

	err = tx.Commit(context.Background())
	if err != nil {
		log.Println("ERROR segment 5: ", err.Error())
		return id, err
	}

	t2 := time.Since(start)
	start = time.Now()

	tx, err = s.db.Begin(context.Background())
	if err != nil {
		log.Println("ERROR segment 1: ", err.Error())
		return id, err
	}

	counterQuery := `UPDATE creator.clients
	SET counter = counter + 1
	WHERE msisdn in (select msisdn from creator.segments where id = $1);`

	//creator.` + idWithoutHyphens + `);`

	_, err = tx.Exec(context.Background(), counterQuery, id)
	if err != nil {
		log.Println("ERROR segment 6: ", err.Error())
		return id, err
	}

	err = tx.Commit(context.Background())
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

	log.Println("Postges times:", t1.Milliseconds(), t2.Milliseconds(),
		t3.Milliseconds(), t4.Milliseconds())

	return id, nil

	//id := uuid.New()
	//
	//tx, err := s.db.Begin()
	//if err != nil {
	//	log.Println("ERROR: postgres MaxOpenConnections:", s.db.Stats().InUse)
	//
	//	return id, err
	//}
	//
	//query := `CALL creator.create_segment($1, $2);`
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

	//rows := [][]interface{}{
	//	{"John", "Smith", int32(36)},
	//	{"Jane", "Doe", int32(29)},
	//}

	//valueStrings := []string{}
	values := [][]interface{}{}
	for _, client := range clients {
		value := []interface{}{
			client.Msisdn, string(client.Gender), client.Age, client.Income, client.Counter,
		}

		values = append(values, value)
		//valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
		//
		//valueArgs = append(valueArgs, client.Msisdn)
		//valueArgs = append(valueArgs, string(client.Gender))
		//valueArgs = append(valueArgs, client.Age)
		//valueArgs = append(valueArgs, client.Income)
		//valueArgs = append(valueArgs, client.Counter)
		//valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
	}

	_, err := s.db.CopyFrom(context.Background(),
		pgx.Identifier{"creator", "clients"},
		[]string{"msisdn", "gender", "age", "income", "counter"},
		pgx.CopyFromRows(values),
	)

	//query := "INSERT INTO creator.clients (msisdn, gender, age, income, counter) VALUES %s"
	//
	//query = fmt.Sprintf(query, strings.Join(valueStrings, ","))
	////fmt.Println("query:", query)
	//
	//tx, err := s.db.Begin(context.Background())
	//if err != nil {
	//	log.Println("ERROR 1: ", err.Error())
	//	return err
	//}
	//
	//_, err = tx.Exec(context.Background(), query, valueArgs...)
	//if err != nil {
	//	log.Println("ERROR 2: ", err.Error())
	//	return err
	//}
	//
	//err = tx.Commit(context.Background())

	if err != nil {
		log.Println("ERROR Copy: ", err.Error())
		return err
	}

	return nil
}
