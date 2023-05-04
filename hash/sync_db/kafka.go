package sync_db

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"log"
	"redis_sync_db/hash/global"
	"redis_sync_db/hash/model"
)

type EventConsumer struct {
	handler sarama.ConsumerGroupHandler
	group   sarama.ConsumerGroup
	topics  []string
	ctx     context.Context
	cancel  context.CancelFunc
}

type EventHandler struct {
	Client    *redis.Client
	PrefixKey string
	CurrentDB *gorm.DB
	lock      *SyncLock
}

func (e EventHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (e EventHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (e EventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	allSync := make(map[string]*model.HSModel)
	batchExcuteCount := 100
	syncCount := 0
	for msg := range claim.Messages() {
		if e.lock.refreshLock() != nil {
			break
		}
		dynamicKey := string(msg.Value)
		newsData, err := e.Client.HGetAll(context.Background(), e.PrefixKey+dynamicKey).Result()
		if err != nil {
			log.Println(err)
			continue
		}
		delete(newsData, global.RedisEmptyFlag)
		if len(newsData) == 0 {
			// 此时对应的dynamicKey的hash已经删除，数据库也要删除
			err := e.CurrentDB.Delete(&model.HSModel{}, "dynamic_key = ?", dynamicKey).Error
			log.Println(err)
			// 如果之前有需要同步dynamic_key的操作，就不需要管他了
			delete(newsData, dynamicKey)
			session.MarkMessage(msg, "")
			session.Commit()
			continue
		}
		marshal, _ := json.Marshal(newsData)
		m := &model.HSModel{
			DynamicKey: dynamicKey,
			Val:        string(marshal),
		}
		allSync[dynamicKey] = m
		// 处理消息成功后标记为处理, 然后会自动提交
		syncCount++
		session.MarkMessage(msg, "")
		if syncCount%batchExcuteCount == 0 {
			if err := doDBSync(e.CurrentDB, allSync); err != nil {
				log.Fatal(err)
			}
			// 重置
			allSync = map[string]*model.HSModel{}
			session.Commit()
		}
	}
	if syncCount != 0 && len(allSync) > 0 {
		if err := doDBSync(e.CurrentDB, allSync); err != nil {
			log.Fatal(err)
		}
	}
	return nil
}

func NewConsumerGroup(group string) sarama.ConsumerGroup {
	ADDR := "192.168.0.56:9092"
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.Retry.Max = 3
	cfg.Consumer.Offsets.AutoCommit.Enable = false // 关闭自动提交，需要手动调用MarkMessage才有效
	client, err := sarama.NewConsumerGroup([]string{ADDR}, group, cfg)
	if err != nil {
		log.Fatal("NewConsumerGroup failed", err.Error())
	}
	return client
}

func NewConsumer(group string, topics []string, handler *EventHandler) *EventConsumer {
	g := NewConsumerGroup(group)
	ctx, cancel := context.WithCancel(context.Background())
	return &EventConsumer{
		handler: handler,
		group:   g,
		topics:  topics,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (e *EventConsumer) consume() {
	for {
		select {
		case <-e.ctx.Done():
			e.group.Close()
			return
		default:
			if err := e.group.Consume(e.ctx, e.topics, e.handler); err != nil {
				log.Println(err)
			}

		}
	}
}
