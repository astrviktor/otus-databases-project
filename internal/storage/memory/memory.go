package memory

import (
	"github.com/google/uuid"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/storage"
)

type Storage struct {
	clients  map[uint64]storage.Client
	segments map[uuid.UUID][]storage.Msisdn
	mutex    *sync.RWMutex
}

func New() *Storage {
	mutex := sync.RWMutex{}

	return &Storage{
		clients:  make(map[uint64]storage.Client),
		segments: make(map[uuid.UUID][]storage.Msisdn),
		mutex:    &mutex,
	}
}

func (s *Storage) CreateConnect() error {
	return nil
}

func (s *Storage) CloseConnect() {
}

func (s *Storage) CreateClients(size int) error {
	s.mutex.Lock()

	var msisdn uint64 = 79000000000
	var gender = [3]rune{'M', 'F', ' '}

	log.Println("creating DB in memory, size: ", size)
	start := time.Now()

	for i := 0; i < size; i++ {
		client := storage.Client{}

		msisdn++

		client.Msisdn = msisdn
		client.Gender = gender[rand.Intn(3)]
		client.Age = rand.Intn(83) + 18
		client.Income = rand.Float64()*90000 + 10000
		client.Next = msisdn + 1

		s.clients[msisdn] = client
	}

	log.Printf("creating DB in memory, time: %v \n", time.Since(start))
	s.mutex.Unlock()

	return nil
}

func (s *Storage) DeleteClients() error {
	s.mutex.Lock()

	for client := range s.clients {
		delete(s.clients, client)
	}

	for segment := range s.segments {
		delete(s.segments, segment)
	}

	s.mutex.Unlock()

	return nil
}

func (s *Storage) CreateSegment(size int) (uuid.UUID, error) {
	s.mutex.Lock()

	uuid := uuid.New()

	clients := make([]storage.Msisdn, size, size)

	var msisdn uint64
	for i := 0; i < size; i++ {
		msisdn = 79000000000 + uint64(i) + 1
		clients[i].Msisdn = s.clients[msisdn].Msisdn
	}
	s.segments[uuid] = clients

	s.mutex.Unlock()

	return uuid, nil
}
