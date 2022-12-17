package memory

import (
	"github.com/google/uuid"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/astrviktor/otus-databases-project/internal/storage"
)

type Storage struct {
	clients  []storage.Client
	segments map[uuid.UUID][]storage.Msisdn
	mutex    *sync.RWMutex
}

func New() *Storage {
	mutex := sync.RWMutex{}

	return &Storage{
		clients:  make([]storage.Client, 0),
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
		client.Age = uint8(rand.Intn(83) + 18)
		client.Income = float32(rand.Intn(9000000)/100 + 10000)
		client.Counter = 0

		s.clients = append(s.clients, client)
	}

	log.Printf("creating DB in memory, time: %v \n", time.Since(start))
	s.mutex.Unlock()

	return nil
}

func (s *Storage) DeleteClients() error {
	s.mutex.Lock()

	s.clients = nil
	//for client := range s.clients {
	//	delete(s.clients, client)
	//}

	for segment := range s.segments {
		delete(s.segments, segment)
	}

	s.mutex.Unlock()

	return nil
}

func (s *Storage) CreateSegment(size int) (uuid.UUID, error) {
	s.mutex.Lock()

	sort.Slice(s.clients, func(i, j int) bool {
		return s.clients[i].Counter < s.clients[j].Counter
	})

	uuid := uuid.New()

	clients := make([]storage.Msisdn, size, size)

	for i := 0; i < size; i++ {
		clients[i].Msisdn = s.clients[i].Msisdn
		s.clients[i].Counter = s.clients[i].Counter + 1
	}
	s.segments[uuid] = clients

	s.mutex.Unlock()

	return uuid, nil
}
