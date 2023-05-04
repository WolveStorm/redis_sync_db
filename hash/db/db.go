package db

import (
	"github.com/Shopify/sarama"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"time"
)

// 初始化一些DB

var Client *redis.Client
var PgDB *gorm.DB
var SyncClient *redis.Client
var KafkaClient sarama.Client

func init() {
	InitPG()
	InitRedis()
	InitKafka()
}

func InitPG() {
	var err error
	PgDB, err = gorm.Open(postgres.New(postgres.Config{
		DSN: "host=localhost user=postgres password=postgres dbname=hash port=5432 sslmode=disable", // DSN data source name
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
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

func InitKafka() {
	var err error
	config := sarama.NewConfig() // 1
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll // 发送完数据需要leader和follower都确认
	// NewRandomPartitioner NewHashPartitioner NewRoundRobinPartitioner NewManualPartitioner NewReferenceHashPartitioner
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// retry
	config.Producer.Retry = struct {
		Max         int
		Backoff     time.Duration
		BackoffFunc func(retries, maxRetries int) time.Duration
	}{
		Max:     3,
		Backoff: 300 * time.Millisecond,
	}
	KafkaClient, err = sarama.NewClient([]string{"192.168.0.56:9092"}, config) // 2
	if err != nil {
		log.Fatal(err)
	}
}
