package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"

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

// func consumer(broker, topic string) {
// 	consumer, err := sarama.NewConsumer([]string{broker}, nil)
// 	if err != nil {
// 		log.Fatalf("Cannot create consumer at %s: %v", broker, err)
// 	}
// 	defer consumer.Close()

// 	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
// 	if err != nil {
// 		log.Fatalf("Cannot create partition consumer at %s: %v", broker, err)
// 	}
// 	defer partitionConsumer.Close()

// 	log.Printf("%s: start receiving messages from %s", topic, broker)
// 	for count := 1; ; count++ {
// 		msg := <-partitionConsumer.Messages()
// 		if count%1000 == 0 {
// 			log.Printf("%s: received %d messages, last (%s)",
// 				topic, count, string(msg.Value))
// 		}
// 	}
// }


// ConsumerGroupHandler implementa el manejo de mensajes de un grupo de consumidores
type ConsumerGroupHandler struct {
	Topic string
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// No se requiere configuración adicional
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	// No se requiere limpieza adicional
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Procesa los mensajes de la partición asignada
	for msg := range claim.Messages() {
		log.Printf("Partition %d: %s", msg.Partition, string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	return nil
}

func consumer(broker, topic string) {
	// Configuración para el consumidor de grupo
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	log.Printf("Start consuming....")
	// Crear el ConsumerGroup
	group, err := sarama.NewConsumerGroup([]string{broker}, "consumer-group-id", config)
	if err != nil {
		log.Fatalf("Error in creating ConsumerGroup: %v", err)
	}
	defer group.Close()

	// Crear el handler para consumir mensajes
	handler := &ConsumerGroupHandler{Topic: topic}

	// Consumir mensajes en un bucle
	for {
		// Iniciar la ingestión de mensajes del ConsumerGroup
		err := group.Consume(context.Background(), []string{topic}, handler)
		if err != nil {
			log.Fatalf("Error in consuming messages: %v", err)
		}
	}
}