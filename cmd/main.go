package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/astrviktor/otus-databases-project/internal/app"
	"github.com/astrviktor/otus-databases-project/internal/config"
	"github.com/astrviktor/otus-databases-project/internal/prometheus"
)

var configFile string

func init() {
	flag.StringVar(&configFile, "config", "config.yaml", "Path to configuration file")
}

func main() {
	flag.Parse()

	config := config.NewConfig(configFile)

	app := app.New(config)

	prometheus.NewPrometheus()

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	app.Start()

	<-exit

	app.Stop()
}
