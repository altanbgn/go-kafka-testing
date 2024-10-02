package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
    p, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "127.0.0.1:9092",
        "acks": "all",
    })

    if err != nil {
        fmt.Printf("%s", err.Error())
        os.Exit(1)
    }

    defer p.Close()

    go func() {
        for e := range p.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
                } else {
                    fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
                        *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
                }
            }
        }
    }()

    topic := "dev-notification"
    message := "hello world"

    err = p.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{
            Topic: &topic,
            Partition: kafka.PartitionAny,
        },
        Value: []byte(message),
    }, nil)
    if err != nil {
        fmt.Printf("%s", err.Error())
        os.Exit(1)
    }

    p.Flush(15 * 1000)
}
