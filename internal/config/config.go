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
	Mysql      DBConfig
	Mongodb    DBConfig
	Clickhouse DBConfig
	Tarantool  DBConfig
	Aerospike  DBConfig
}

type HTTPServerConfig struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

type DBConfig struct {
	Host               string `yaml:"host"`
	Port               int    `yaml:"port"`
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
		Mysql: DBConfig{DSN: "user:password@tcp(10.5.0.7:3306)/creator",
			MaxConnectAttempts: 5},
		Mongodb: DBConfig{DSN: "mongodb://root:password@10.5.0.8:27017",
			MaxConnectAttempts: 5},
		Clickhouse: DBConfig{DSN: "10.5.0.10:9000",
			MaxConnectAttempts: 5},
		Tarantool: DBConfig{DSN: "10.5.0.11:3301",
			MaxConnectAttempts: 5},
	}
}
