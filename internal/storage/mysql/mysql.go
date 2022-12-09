package mysql

import (
	"database/sql"
	"log"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/config"
	_ "github.com/go-sql-driver/mysql" //nolint
	"github.com/google/uuid"
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
	if err := s.db.Close(); err != nil {
		log.Printf("failed to close db: %s", err)
	}
}

func (s *Storage) CreateClients(size int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	query := `CALL create_clients(?);`

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

	tx, err := s.db.Begin()
	if err != nil {
		log.Println("ERROR: MySQL MaxOpenConnections:", s.db.Stats().InUse)

		return id, err
	}

	query := `CALL create_segment(UUID_TO_BIN(?), ?);`

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
