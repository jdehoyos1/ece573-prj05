package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

func main() {
	topic := os.Getenv("TOPIC")
	if topic == "" {
		log.Fatalf("Unknown topic")
	}

	role := os.Getenv("ROLE")
	broker := os.Getenv("KAFKA_BROKER")

	if role == "producer" {
		producer(broker, topic)
	} else if role == "consumer" {
		consumer(broker, topic)
	} else {
		log.Fatalf("Unknown role %s", role)
	}
}

func producer(broker, topic string) {
	producer, err := sarama.NewSyncProducer([]string{broker}, nil)
	if err != nil {
		log.Fatalf("Cannot create producer at %s: %v", broker, err)
	}
	defer producer.Close()

	log.Printf("%s: start publishing messages to %s", topic, broker)
	for count := 1; ; count++ {
		value := rand.Float64()
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("%f", value)),
		}

		_, _, err = producer.SendMessage(message)
		if err != nil {
			log.Fatalf("Cannot publish message %d (%f) to %s: %v",
				count, value, topic, err)
		}

		if count%1000 == 0 {
			log.Printf("%s: %d messages published", topic, count)
		}
	}
}

func consumer(broker, topic string) {
	consumer, err := sarama.NewConsumer([]string{broker}, nil)
	if err != nil {
		log.Fatalf("Cannot create consumer at %s: %v", broker, err)
	}
	defer consumer.Close()

	// Obtener las particiones del tema
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("Failed to get partitions for topic %s: %v", topic, err)
	}

	log.Printf("%s: consuming messages from all partitions", topic)

	var wg sync.WaitGroup
	messageCh := make(chan *sarama.ConsumerMessage, len(partitions))

	// Consumir mensajes de todas las particiones
	for _, partition := range partitions {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			consumePartition(consumer, topic, partition, messageCh)
		}(partition)
	}

	// Procesar los mensajes recibidos
	go func() {
		wg.Wait()
		close(messageCh)
	}()

	count := 0
	for msg := range messageCh {
		count++
		if count%1000 == 0 {
			log.Printf("%s: received %d messages, last (%s)",
				topic, count, string(msg.Value))
		}
	}
}

func consumePartition(consumer sarama.Consumer, topic string, partition int32, messageCh chan<- *sarama.ConsumerMessage) {
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Cannot create partition consumer for partition %d: %v", partition, err)
		return
	}
	defer partitionConsumer.Close()

	for msg := range partitionConsumer.Messages() {
		messageCh <- msg
	}
}
