package sync_db

import (
	"log"
	"redis_sync_db/hash/config"
	"redis_sync_db/hash/model"
	"sync"
)

var (
	onceInit sync.Once
)

var SyncConfigMap map[string]*config.HSSyncConfig

func init() {
	// 初始化map
	InitSyncMap()
}

// 初始化同步相关
func InitSyncManager() {
	// 只初始化一次
	onceInit.Do(func() {
		// 可能需要更新map
		InitSyncMap()
		// 可能需要创建新的db
		TryCreateDBTable()
	})
}

// 初始化同步map
func InitSyncMap() {
	SyncConfigMap = make(map[string]*config.HSSyncConfig)
	for _, cfg := range config.SyncConfigs {
		SyncConfigMap[cfg.PrefixKey] = cfg
	}
}

// 尝试为redis对应prefix key的hash进行创建pg表
func TryCreateDBTable() {
	for _, cfg := range SyncConfigMap {
		// 不存在时创建
		if !cfg.PgDB.Migrator().HasTable(cfg.TableName) {
			// 根据cfg的TableName，创建出对应的PG数据库
			err := cfg.PgDB.Table(cfg.TableName).Migrator().CreateTable(&model.HSModel{})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
