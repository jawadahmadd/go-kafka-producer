package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func main(){
	brokerList := []string{"localhost:9092"}
	topic := "jawad" // any topic you want
	producer := &Producer{
		SyncProducer:  GetSyncProducer(brokerList),
		AsyncProducer: GetAsyncProducer(brokerList),
	}
	ProduceMessageAsync(producer, topic)
	ProduceMessageSync(producer, topic)

	producer.SyncProducer.Close() // handle error yourself
	producer.AsyncProducer.Close() // // handle error yourself
}


type Producer struct {
	SyncProducer     sarama.SyncProducer
	AsyncProducer sarama.AsyncProducer
}

func ProduceMessageAsync(producer *Producer, topic string){
	// We are not setting a message key, which means that all messages will
	// be distributed randomly over the different partitions.
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Hello Go! Async Message"),
	}
	producer.AsyncProducer.Input() <- message
}


func ProduceMessageSync(producer *Producer, topic string){
	// We are not setting a message key, which means that all messages will
	// be distributed randomly over the different partitions.
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Hello Go! Sync Message"),
	}
	partition, offset, err:= producer.SyncProducer.SendMessage(message)

	if err != nil {
		fmt.Println( "Failed to store your data:, %s", err)
	} else {
		// The tuple (topic, partition, offset) can be used as a unique identifier
		// for a message in a Kafka cluster.
		fmt.Println( "Your data is stored with unique identifier important/%d/%d", partition, offset)
	}
}

func GetAsyncProducer(brokerList []string) sarama.AsyncProducer {
	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}
	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer
}

func GetSyncProducer(brokerList []string) sarama.SyncProducer {
	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}