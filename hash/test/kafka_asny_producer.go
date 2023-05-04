package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	var (
		wg                     sync.WaitGroup
		success_num, error_num int
	)
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

	client, err := sarama.NewClient([]string{"192.168.124.9:9092"}, config) // 2
	if err != nil {
		panic(err)
	}
	defer client.Close()
	producer, err := sarama.NewAsyncProducerFromClient(client) // 3
	if err != nil {
		panic(err)
	}
	defer producer.AsyncClose()
	wg.Add(1)
	go func() {
		wg.Done()
		// config.Producer.Return.Successes = true 后一定要监听这个chan，默认大小256 如果满了就阻塞掉
		for range producer.Successes() {
			fmt.Println("success")
			success_num++
		}
	}()
	wg.Add(1)
	go func() {
		wg.Done()
		// config.Producer.Return.Errors = true 后一定要监听这个chan，默认大小256 如果满了就阻塞掉
		for range producer.Errors() {
			// 这里可以处理回调
			error_num++
		}
	}()
	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
ProducerLoop:
	for {
		message := &sarama.ProducerMessage{Topic: "sync", Value: sarama.StringEncoder("hhhhh")} // 4
		select {
		case producer.Input() <- message:

		case <-signals:
			// Trigger a shutdown of the producer.
			break ProducerLoop
		}
		time.Sleep(5 * time.Second)
	}

	wg.Wait()
	time.Sleep(time.Second * 2)
	log.Printf("Successfully produced: %d; errors: %d\n", success_num, error_num)

}
