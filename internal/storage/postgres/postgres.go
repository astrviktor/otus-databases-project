package postgres

import (
	"database/sql"
	"log"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/config"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/stdlib" //nolint
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
	db, err := sql.Open("pgx", s.dsn)
	if err != nil {
		return err
	}

	for i := 0; i < s.dbMaxConnectAttempts; i++ {
		log.Println("trying to connect to Postgres...")
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

	log.Println("connect to Postgres status OK")

	return nil
}

func (s *Storage) CloseConnect() {
	if err := s.db.Close(); err != nil {
		log.Printf("failed to close db: %s", err)
	}
}

func (s *Storage) CreateClients(size int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	query := `CALL creator.create_clients($1);`

	_, err = tx.Exec(query, size)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) DeleteClients() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	query := `CALL creator.delete_clients();`

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

	tx, err := s.db.Begin()
	if err != nil {
		log.Println("ERROR: postgres MaxOpenConnections:", s.db.Stats().InUse)

		return id, err
	}

	query := `CALL creator.create_segment($1, $2);`

	_, err = tx.Exec(query, id, size)
	if err != nil {
		return id, err
	}

	err = tx.Commit()
	if err != nil {
		return id, err
	}

	return id, nil

}
