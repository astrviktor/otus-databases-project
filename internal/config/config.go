package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	HTTPServer HTTPServerConfig
	Memory     DBConfig
	Postgres   DBConfig
}

type HTTPServerConfig struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

type DBConfig struct {
	DSN                string `yaml:"dsn"`
	MaxConnectAttempts int    `yaml:"maxConnectAttempts"`
}

func NewConfig(name string) Config {
	var config Config

	file, err := os.ReadFile(name)
	if err != nil {
		log.Println(err.Error())
		return DefaultConfig()
	}

	err = yaml.Unmarshal(file, &config)
	if err != nil {
		log.Println(err.Error())
		return DefaultConfig()
	}

	return config
}

func DefaultConfig() Config {
	log.Println("get default config")

	return Config{
		HTTPServer: HTTPServerConfig{Host: "", Port: "8888"},
		Memory: DBConfig{DSN: "",
			MaxConnectAttempts: 0},
		Postgres: DBConfig{DSN: "postgres://user:password@postgres:5432/creator",
			MaxConnectAttempts: 5},
	}
}
