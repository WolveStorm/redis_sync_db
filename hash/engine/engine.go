package engine

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"log"
	"redis_sync_db/hash/global"
	"redis_sync_db/hash/sync_db"
	"time"
)

// HSEngine 包装一层redis,增加一些读回调和写回调
type HSEngine struct {
	*redis.Client        // redis Client
	hsKey         string // hash key
	prefixKey     string
	dynamicKey    string        // 用于标识hsKey的一条记录，可能是ID等
	expireTime    time.Duration // 缓存过期时间
}

// NewHSEngine new engine
func NewHSEngine(client *redis.Client, prefixKey, dynamicKey string, expireTime time.Duration) *HSEngine {
	engine := &HSEngine{
		Client:     client,
		dynamicKey: dynamicKey,
		prefixKey:  prefixKey,
		hsKey:      prefixKey + dynamicKey,
		expireTime: expireTime,
	}
	// 尝试刷新缓存或者重新载入缓存
	engine.loadOrRefreshExpiration()
	return engine
}

func (t *HSEngine) loadOrRefreshExpiration() {
	//key 不存在返回 -2
	//key 存在但是没有关联超时时间返回 -1
	ttl := t.TTL(context.Background(), t.hsKey).Val()
	if ttl == -2*time.Nanosecond {
		// 重新从数据库load
		res, err := t.ReadFromDB()
		if err != nil {
			log.Fatal(err)
			return
		}
		if len(res) == 0 {
			res = map[string]interface{}{
				global.RedisEmptyFlag: "",
			}
		}
		err = t.Client.HMSet(context.Background(), t.hsKey, res).Err()
		if err != nil {
			return
		}
		t.RefreshExpiration()
	}
	t.RefreshExpiration()
}

func (t *HSEngine) ReadFromDB() (map[string]interface{}, error) {
	sync_db.InitSyncManager()
	// 找到该hset对应的conf
	conf := sync_db.SyncConfigMap[t.prefixKey]
	res := make(map[string]interface{})
	err := conf.CurrentDB().Find(&res, "dynamic_key = ?", t.dynamicKey).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (t *HSEngine) isEmpty(ctx context.Context) bool {
	return t.HExists(ctx, global.RedisEmptyFlag).Val()
}

func (t *HSEngine) RefreshExpiration() {
	t.Expire(context.Background(), t.hsKey, t.expireTime)
}

func (t *HSEngine) HDel(ctx context.Context, fields ...string) *redis.IntCmd {
	defer t.writeCallback(ctx)
	return t.Client.HDel(ctx, t.hsKey, fields...)
}

func (t *HSEngine) HExists(ctx context.Context, field string) *redis.BoolCmd {
	defer t.readCallback()
	return t.Client.HExists(ctx, t.hsKey, field)
}

func (t *HSEngine) HGet(ctx context.Context, field string) *redis.StringCmd {
	defer t.readCallback()
	return t.Client.HGet(ctx, t.hsKey, field)
}

func (t *HSEngine) HGetAll(ctx context.Context) *redis.MapStringStringCmd {
	defer t.readCallback()

	if t.isEmpty(ctx) {
		return redis.NewMapStringStringCmd(ctx, "hgetall", t.hsKey)
	}

	return t.Client.HGetAll(ctx, t.hsKey)
}

func (t *HSEngine) HIncrBy(ctx context.Context, field string, incr int64) *redis.IntCmd {
	defer t.writeCallback(ctx)
	return t.Client.HIncrBy(ctx, t.hsKey, field, incr)
}

func (t *HSEngine) HIncrByFloat(ctx context.Context, field string, incr float64) *redis.FloatCmd {
	defer t.writeCallback(ctx)
	return t.Client.HIncrByFloat(ctx, t.hsKey, field, incr)
}

func (t *HSEngine) HKeys(ctx context.Context) *redis.StringSliceCmd {
	defer t.readCallback()

	if t.isEmpty(ctx) {
		return redis.NewStringSliceCmd(ctx, "hkeys", t.hsKey)
	}
	return t.Client.HKeys(ctx, t.hsKey)
}

func (t *HSEngine) HLen(ctx context.Context) *redis.IntCmd {
	defer t.readCallback()

	if t.isEmpty(ctx) {
		return redis.NewIntCmd(ctx, "hlen", t.hsKey)
	}

	return t.Client.HLen(ctx, t.hsKey)
}

func (t *HSEngine) HMGet(ctx context.Context, fields ...string) *redis.SliceCmd {
	defer t.readCallback()
	return t.Client.HMGet(ctx, t.hsKey, fields...)
}

func (t *HSEngine) HMSet(ctx context.Context, fields map[string]interface{}) *redis.BoolCmd {
	defer t.writeCallback(ctx)
	return t.Client.HMSet(ctx, t.hsKey, fields)
}

func (t *HSEngine) HScan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	defer t.readCallback()

	if t.isEmpty(ctx) {
		return new(redis.ScanCmd)
	}

	return t.Client.HScan(ctx, t.hsKey, cursor, match, count)
}

func (t *HSEngine) HSet(ctx context.Context, field string, value interface{}) *redis.IntCmd {
	defer t.writeCallback(ctx)
	return t.Client.HSet(ctx, t.hsKey, field, value)
}

func (t *HSEngine) HSetNX(ctx context.Context, field string, value interface{}) *redis.BoolCmd {
	defer t.writeCallback(ctx)
	return t.Client.HSetNX(ctx, t.hsKey, field, value)
}

func (t *HSEngine) HVals(ctx context.Context) *redis.StringSliceCmd {
	defer t.readCallback()

	if t.isEmpty(ctx) {
		return redis.NewStringSliceCmd(ctx, "hvals", t.hsKey)
	}

	return t.Client.HVals(ctx, t.hsKey)
}

// write call back
func (t *HSEngine) writeCallback(ctx context.Context) {
	// 一个engine对应一条hash记录，我们需要将对应的记录被修改这个消息设置到消息队列中
	defer func() {
		if err := t.insertWriteMsgToSyncList(ctx); err != nil {
			fmt.Println(err)
		}
	}()
	// step1 刷新缓存
	t.RefreshExpiration()
	l, err := t.Client.HLen(ctx, t.hsKey).Result()
	// step2 判断此时hset有没有元素,设置空标志
	if err == nil && l == 0 {
		t.Client.HSet(ctx, t.hsKey, global.RedisEmptyFlag, "")
		return
	}
	// step3 判断是否需要移除空标志，若当前存在空标志，但key数量大于1，说明有新的元素
	if t.isEmpty(ctx) && l > 1 {
		t.Client.HDel(ctx, t.hsKey, global.RedisEmptyFlag)
		return
	}
}

// read call back
func (t *HSEngine) readCallback() {
	// 每次进行redis读取，都会进行刷新
	t.RefreshExpiration()
}

// add msg to msg queue
// TODO 解决消息丢失的问题
func (t *HSEngine) insertWriteMsgToSyncList(ctx context.Context) error {
	cfg := sync_db.SyncConfigMap[t.prefixKey]
	return cfg.SyncClient.SAdd(ctx, cfg.SyncKey, t.dynamicKey).Err()
}
