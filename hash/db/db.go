package db

import (
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
)

// 初始化一些DB

var Client *redis.Client
var PgDB *gorm.DB
var SyncClient *redis.Client

func init() {
	InitPG()
	InitRedis()
}

func InitPG() {
	var err error
	PgDB, err = gorm.Open(postgres.New(postgres.Config{
		DSN: "host=localhost user=postgres password=postgres dbname=hash port=5432 sslmode=disable", // DSN data source name
	}), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
}

func InitRedis() {
	Client = redis.NewClient(&redis.Options{
		Addr:     "0.0.0.0:6379",
		DB:       0,
		Password: "123456",
	})
	SyncClient = redis.NewClient(&redis.Options{
		Addr:     "0.0.0.0:6379",
		DB:       0,
		Password: "123456",
	})
}
