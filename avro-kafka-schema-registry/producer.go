package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type ComplexType struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func main() {

	topic := "myTopic"

	// 1) Create the producer as you would normally do using Confluent's Go client
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for event := range p.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				message := ev
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Error delivering the message '%s'\n", message.Key)
				} else {
					fmt.Printf("Message '%s' delivered successfully!\n", message.Key)
				}
			}
		}
	}()

	// 2) Fetch the latest version of the schema, or create a new one if it is the first
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	schema, err := schemaRegistryClient.GetLatestSchema(topic)
	fmt.Printf("Schema : %s", schema.Schema())
	if schema == nil {
		schemaBytes, _ := os.ReadFile("hello.avsc")
		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), srclient.Avro)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	// 3) Serialize the record using the schema provided by the client,
	// making sure to include the schema id as part of the record.
	newComplexType := ComplexType{ID: 3, Name: "Jitendra"}
	value, _ := json.Marshal(newComplexType)
	native, _, _ := schema.Codec().NativeFromTextual(value)
	valueBytes, _ := schema.Codec().BinaryFromNative(nil, native)

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)

	key, _ := uuid.NewUUID()
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(key.String()), Value: recordValue}, nil)

	p.Flush(15 * 1000)

}
