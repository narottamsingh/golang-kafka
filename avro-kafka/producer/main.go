package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}

	defer p.Close()

	topic := "golangproducer"
	//schemaRegistryURL := "http://localhost:8081"

	avroSchemaJSON := `{
		"type": "record",
		"name": "example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	codec, err := goavro.NewCodec(avroSchemaJSON)
	if err != nil {
		fmt.Printf("Failed to create Avro codec: %s\n", err)
		return
	}

	data := map[string]interface{}{
		"id":   1,
		"name": "John",
	}
	fmt.Printf("data is %s\n", data)
	key := []byte("19")
	value, err := codec.TextualFromNative(nil, data)
	if err != nil {
		fmt.Printf("Failed to encode Avro data: %s\n", err)
		return
	}

	fmt.Printf("values 1 %s\n", value)
	deliveryChan := make(chan kafka.Event)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Headers:        []kafka.Header{{Key: "Content-Type", Value: []byte("avro")}},
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
}
