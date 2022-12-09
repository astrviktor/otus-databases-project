package internalhttp

import (
	"context"
	"errors"
	"github.com/astrviktor/otus-databases-project/internal/config"
	"github.com/astrviktor/otus-databases-project/internal/storage/mongodb"
	"github.com/astrviktor/otus-databases-project/internal/storage/mysql"
	"github.com/astrviktor/otus-databases-project/internal/storage/postgres"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/storage"
)

type Server struct {
	config  config.Config
	wg      *sync.WaitGroup
	srv     *http.Server
	storage storage.Storage
}

func NewServer(config config.Config, storage storage.Storage) *Server {
	return &Server{
		config:  config,
		wg:      &sync.WaitGroup{},
		srv:     &http.Server{},
		storage: storage,
	}
}

func (s *Server) ChangeDatabase(database string) error {
	if database == "postgres" {
		s.storage.CloseConnect()
		s.storage = postgres.New(s.config.Postgres)
		err := s.storage.CreateConnect()
		if err != nil {
			return err
		}

		log.Println("database: postgres")
		return nil
	}

	if database == "mysql" {
		s.storage.CloseConnect()
		s.storage = mysql.New(s.config.Mysql)
		err := s.storage.CreateConnect()
		if err != nil {
			log.Println("MySQL create connection error:", err)
			return err
		}

		log.Println("database: mysql")
		return nil
	}

	if database == "mongodb" {
		s.storage.CloseConnect()
		s.storage = mongodb.New(s.config.Mongodb)
		err := s.storage.CreateConnect()
		if err != nil {
			log.Println("MongoDB create connection error:", err)
			return err
		}

		log.Println("database: mongodb")
		return nil
	}

	return errors.New("error: trying to choose unknown database")
}

func (s *Server) Start() {
	if err := s.storage.CreateConnect(); err != nil {
		log.Fatalf("Storage Connect(): %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/database/", Logging(s.handleDatabase))
	mux.HandleFunc("/clients/", Logging(s.handleCreate))
	mux.HandleFunc("/clients", Logging(s.handleDelete))
	mux.HandleFunc("/segment/", Logging(s.handleSegment))

	mux.Handle("/metrics", promhttp.Handler())

	addr := net.JoinHostPort(s.config.HTTPServer.Host, s.config.HTTPServer.Port)

	s.srv = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	log.Println("http server starting on address: " + addr)

	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		if err := s.srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("ListenAndServe(): %v", err)
		}
		log.Println("http server stopping")
	}()
}

func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := s.srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server Shutdown(): %v", err)
	}

	s.storage.CloseConnect()

	defer cancel()

	// Wait for ListenAndServe goroutine to close.
	s.wg.Wait()
	log.Println("http server gracefully shutdown")
}
