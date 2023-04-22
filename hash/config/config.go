package config

import (
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"redis_sync_db/hash/db"
)

// 同步到PG需要的一些信息
type HSSyncConfig struct {
	Client     *redis.Client
	TableName  string
	PgDB       *gorm.DB
	SyncClient *redis.Client
	SyncKey    string
	PrefixKey  string
}

// 根据TableName返回DB
func (hash *HSSyncConfig) CurrentDB() *gorm.DB {
	return hash.PgDB.Table(hash.TableName)
}

var (
	testConfig = &HSSyncConfig{
		Client:     db.Client,
		TableName:  "data",
		PgDB:       db.PgDB,
		SyncClient: db.SyncClient,
		SyncKey:    "key:sync_data:",
		PrefixKey:  "hash:data:",
	}
)

var SyncConfigs = []*HSSyncConfig{
	testConfig,
}
