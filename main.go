package main

import (
	"context"
	"github.com/robfig/cron/v3"
	"os"
	"os/signal"
	"redis_sync_db/hash/db"
	"redis_sync_db/hash/engine"
	"redis_sync_db/hash/sync_db"
	"sync"
	"syscall"
	"time"
)

func main() {
	c := cron.New()
	sc := make(chan os.Signal)
	tool := &sync_db.SyncTool{WG: new(sync.WaitGroup)}
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	c.AddFunc("@every 1m", tool.SyncToDB)
	userId := "100001"
	u := engine.NewHSEngine(db.Client, "hash:data:", userId, 7*time.Minute*60*24)
	// test case
	u.HSet(context.Background(), "name", "phm")
	c.Start()
	<-sc
}
