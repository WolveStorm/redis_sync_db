package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"redis_sync_db/hash/db"
	"redis_sync_db/hash/model"
	"redis_sync_db/hash/sync_db"
	"testing"
	"time"
)

func Test(t *testing.T) {
	t.Run("gen_data", func(t *testing.T) {
		db.InitPG()
		db.InitRedis()
		userId := "100001"
		u := NewHSEngine(db.Client, "hash:data:", userId, time.Minute, false)
		u.HSet(context.Background(), "name", "phm")
		m := map[string]interface{}{
			"name": "phm",
		}
		marshal, _ := json.Marshal(m)
		config := sync_db.SyncConfigMap[u.prefixKey]
		config.CurrentDB().Create(&model.HSModel{
			DynamicKey: "100001",
			Val:        string(marshal),
		})
	})
	// test migration
	t.Run("test migration", func(t *testing.T) {
		db.InitPG()
		sync_db.InitSyncMap()
		sync_db.TryCreateDBTable()
	})
	// test readFromDB
	t.Run("test readFromDB", func(t *testing.T) {
		db.InitPG()
		db.InitRedis()
		userId := "100001"
		u := NewHSEngine(db.Client, "hash:data:", userId, time.Minute, false)
		fromDB, err := u.ReadFromDB()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(fromDB)
	})
	// test send msg
	t.Run("test sync_db", func(t *testing.T) {
		db.InitPG()
		db.InitRedis()
		userId := "100001"
		u := NewHSEngine(db.Client, "hash:data:", userId, time.Minute, true)
		u.syncer.insertWriteMsgToSyncList(context.Background(), u.prefixKey, u.dynamicKey)
		time.Sleep(5 * time.Second)
	})

	// test hset
	t.Run("test hset", func(t *testing.T) {
		db.InitPG()
		db.InitRedis()
		userId := "100001"
		u := NewHSEngine(db.Client, "hash:data:", userId, time.Minute, false)
		u.HSet(context.Background(), "name", "phm")
	})

}
