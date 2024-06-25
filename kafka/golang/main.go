package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	// 设置Kafka消费者配置
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// 创建Kafka消费者对象
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln("Failed to start consumer:", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln("Failed to close consumer:", err)
		}
	}()

	// 订阅指定topic
	topic := "my-topic"
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalln("Failed to get partitions for topic", topic, ":", err)
	}

	// 创建等待组
	var wg sync.WaitGroup
	wg.Add(len(partitionList))

	// 创建结束标识通道
	done := make(chan struct{})

	// 循环处理每个partition
	for _, partition := range partitionList {
		// 创建partition消费者对象
		partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalln("Failed to start consumer for partition", partition, ":", err)
		}

		go func(partitionConsumer sarama.PartitionConsumer, partition int32) {
			defer wg.Done()

			// 循环读取消息
			for message := range partitionConsumer.Messages() {
				fmt.Println("Received message from partition", partition, ":", string(message.Value))
			}

			// 关闭partition消费者对象
			if err := partitionConsumer.Close(); err != nil {
				log.Fatalln("Failed to close partition consumer:", err)
			}
		}(partitionConsumer, partition)
	}

	// 等待所有协程结束或接收到终止信号
	go func() {
		wg.Wait()
		close(done)
	}()
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		log.Println("Received termination signal, waiting for all goroutines to exit...")
	case <-done:
		log.Println("All goroutines have exited.")
	}

}
