package sync_db

import (
	"context"
	"encoding/json"
	"github.com/joyous-x/saturn/common/safego"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"log"
	"redis_sync_db/hash/config"
	"redis_sync_db/hash/global"
	"redis_sync_db/hash/model"
	"sync"
	"time"
)

// for cron
type SyncTool struct {
	WG *sync.WaitGroup
}

type SyncLock struct {
	lockClient *redis.Client
	lockName   string
	timeExpire time.Duration
}

var lockPrefix = "sync_db:lock:"

// 希望锁住正在同步的hash
func applyLock(client *redis.Client, prefixKey string) *SyncLock {
	applySuccess := false
	for i := 0; i < 5; i++ {
		err := client.SetNX(context.Background(), lockPrefix+prefixKey, time.Now(), 5*time.Minute).Err()
		if err != nil {
			time.Sleep(1)
			continue
		}
		applySuccess = true
		break
	}
	if applySuccess {
		return &SyncLock{
			lockClient: client,
			lockName:   lockPrefix + prefixKey,
			timeExpire: 5 * time.Second,
		}
	}
	return nil
}

func (s *SyncLock) returnLock() {
	s.lockClient.Del(context.Background(), s.lockName)
}

func (s *SyncLock) refreshLock() error {
	return s.lockClient.Expire(context.Background(), s.lockName, s.timeExpire).Err()
}

func (s *SyncTool) SyncToDB() {
	s.WG.Add(len(SyncConfigMap))
	for _, cfg := range SyncConfigMap {
		safego.Go(func() {
			s.doSyncToDB(cfg)
		})
	}
	s.WG.Wait()
}

func (s *SyncTool) doSyncToDB(cfg *config.HSSyncConfig) {
	defer s.WG.Done()
	if cfg == nil {
		return
	}
	lock := applyLock(cfg.SyncClient, cfg.PrefixKey)
	if lock == nil {
		// 无法申请到锁
		return
	}
	// 同步完成归还锁
	defer lock.returnLock()
	allSync := make(map[string]*model.HSModel)
	sleepCount := 0
	maxSleep := 3
	batchExcuteCount := 100
	syncCount := 0
	for {
		if lock.refreshLock() != nil {
			break
		}
		dynamicKey, err := cfg.SyncClient.SPop(context.Background(), cfg.SyncKey).Result()
		if err == redis.Nil {
			// 同步完成,再循环重试几次，实在没有就退出
			if sleepCount >= maxSleep {
				return
			}
			sleepCount++
			time.Sleep(1 * time.Second)
			break
		}
		if err != nil {
			// 防止丢数据
			cfg.SyncClient.SAdd(context.Background(), cfg.SyncKey)
			continue
		}
		// 从redis拿到最新的数据
		newsData, err := cfg.Client.HGetAll(context.Background(), cfg.PrefixKey+dynamicKey).Result()
		if err != nil && err != redis.Nil {
			// 防止丢数据
			cfg.SyncClient.SAdd(context.Background(), cfg.SyncKey)
			continue
		}
		delete(newsData, global.RedisEmptyFlag)
		if len(newsData) == 0 {
			// 此时对应的dynamicKey的hash已经删除，数据库也要删除
			err := cfg.CurrentDB().Delete(&model.HSModel{}, "dynamic_key = ?", dynamicKey).Error
			log.Println(err)
			// 如果之前有需要同步dynamic_key的操作，就不需要管他了
			delete(newsData, dynamicKey)
			continue
		}
		marshal, _ := json.Marshal(newsData)
		m := &model.HSModel{
			DynamicKey: dynamicKey,
			Val:        string(marshal),
		}
		allSync[dynamicKey] = m
		syncCount++
		// 批量同步
		if syncCount%batchExcuteCount == 0 {
			if err := doDBSync(cfg.CurrentDB(), allSync); err != nil {
				log.Fatal(err)
			}
			// 重置
			allSync = map[string]*model.HSModel{}
		}
	}
	if len(allSync) != 0 {
		// 同步
		if err := doDBSync(cfg.CurrentDB(), allSync); err != nil {
			log.Fatal(err)
		}
	}
}

func doDBSync(db *gorm.DB, syncMap map[string]*model.HSModel) error {
	syncList := make([]*model.HSModel, 0)
	for _, m := range syncMap {
		syncList = append(syncList, m)
	}
	// create or conflict do update
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "dynamic_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"val"}),
	}).Create(&syncList).Error
}
