package app

import (
	"github.com/astrviktor/otus-databases-project/internal/config"
	internalhttp "github.com/astrviktor/otus-databases-project/internal/server/http"
	"github.com/astrviktor/otus-databases-project/internal/storage"
	"github.com/astrviktor/otus-databases-project/internal/storage/memory"
	//sqlstorage "github.com/astrviktor/otus-databases-project/internal/storage/sql"
)

type App struct {
	server *internalhttp.Server
}

func New(config config.Config) *App {
	var storage storage.Storage = memory.New()

	server := internalhttp.NewServer(config, storage)
	return &App{server}
}

func (a *App) Start() {
	a.server.Start()
}

func (a *App) Stop() {
	a.server.Stop()
}
