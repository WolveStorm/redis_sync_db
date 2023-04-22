package main

import (
	"github.com/robfig/cron/v3"
	"os"
	"os/signal"
	"redis_sync_db/hash/sync_db"
	"sync"
	"syscall"
)

func main() {
	c := cron.New()
	sc := make(chan os.Signal)
	tool := &sync_db.SyncTool{WG: new(sync.WaitGroup)}
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	c.AddFunc("@every 1s", tool.SyncToDB)
	// for test
	//userId := "100001"
	//u := engine.NewHSEngine(db.Client, "hash:data:", userId, 7*time.Minute*60*24)
	//test case
	//u.HSet(context.Background(), "name", "phm")
	// test case
	//u.HDel(context.Background(), "name")
	c.Start()
	<-sc
}
