package kcopy

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

type KafkaConfig struct {
	Addrs    []string
	KeyValue []string
}

func (k *KafkaConfig) ToSaramaConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()
	for _, kv := range k.KeyValue {
		key, value, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("invalid key value pair %s", kv)
		}
		switch key {
		case "tls.enable":
			boolValue, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("invalid value for tls.enable %s, expected true/false", value)
			}
			config.Net.TLS.Enable = boolValue
		case "sasl.mechanism":
			switch value {
			case "PLAIN":
				config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			default:
				return nil, fmt.Errorf("unsupported sasl.mechanism %s", value)
			}
			config.Net.SASL.Enable = true
		case "sasl.user":
			config.Net.SASL.User = value
		case "sasl.password":
			config.Net.SASL.Password = value
		}
	}
	return config, nil
}

type KafkaTopic struct {
	Input       string
	Source      string
	Destination string
}

func (k *KafkaTopic) Parse() error {
	if k.Input == "" {
		return fmt.Errorf("invalid topic format %s", k.Input)
	}
	source, destination, ok := strings.Cut(k.Input, ":")
	if !ok {
		destination = source
	}
	k.Source = source
	k.Destination = destination
	return nil
}

type KafkaPartition struct {
	Input       string
	Source      int32
	Destination int32
}

func (k *KafkaPartition) Parse() error {
	if k.Input == "" {
		// It mostly means from 0 to 0.
		return nil
	}
	source, destination, ok := strings.Cut(k.Input, ":")
	sourceInt, err := strconv.Atoi(source)
	if err != nil {
		return fmt.Errorf("invalid partition source %s", source)
	}
	if !ok {
		k.Source = int32(sourceInt)
		return nil
	}
	destinationInt, err := strconv.Atoi(destination)
	if err != nil {
		return fmt.Errorf("invalid partition destination %s", destination)
	}
	k.Destination = int32(destinationInt)
	return nil
}

type KCopy struct {
	Source      *KafkaConfig
	Destination *KafkaConfig
	Topic       *KafkaTopic
	Partition   *KafkaPartition
	Offset      int64
	Count       int64
}

func NewKCopy() *KCopy {
	return &KCopy{
		Source:      &KafkaConfig{},
		Destination: &KafkaConfig{},
		Topic:       &KafkaTopic{},
		Partition:   &KafkaPartition{},
	}
}

func (k *KCopy) Copy() (func(), error) {
	destinationConfig, err := k.Destination.ToSaramaConfig()
	if err != nil {
		return nil, err
	}
	sourceConfig, err := k.Source.ToSaramaConfig()
	if err != nil {
		return nil, err
	}
	if err := k.Topic.Parse(); err != nil {
		return nil, err
	}
	if err := k.Partition.Parse(); err != nil {
		return nil, err
	}
	producer, err := sarama.NewAsyncProducer(k.Destination.Addrs, destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to start producer: %w", err)
	}
	consumer, err := sarama.NewConsumer(k.Source.Addrs, sourceConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to start consumer: %w", err)
	}

	// k.Offset is -1 means latest, -2 means oldest
	pc, err := consumer.ConsumePartition(k.Topic.Source, k.Partition.Source, k.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to consume partition %v: %w", k.Partition.Source, err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	var done = make(chan os.Signal, 2)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Printf("failed to produce message: %v", err)
			done <- syscall.SIGINT
		}
	}()
	go func() {
		var count int64
		defer producer.AsyncClose()
		defer wg.Done()
		defer func() { log.Printf("copied %d messages", count) }()
		log.Printf("copying %d messages from %s (%s/p-%d/o-%d) to %s (%s/p-%d)", k.Count, strings.Join(k.Source.Addrs, ", "), k.Topic.Source, k.Partition.Source, k.Offset, strings.Join(k.Destination.Addrs, ", "), k.Topic.Destination, k.Partition.Destination)
		for {
			select {
			case <-done:
				return
			case message := <-pc.Messages():
				producer.Input() <- &sarama.ProducerMessage{
					Topic:     k.Topic.Destination,
					Key:       sarama.ByteEncoder(message.Key),
					Value:     sarama.ByteEncoder(message.Value),
					Partition: k.Partition.Destination,
				}
				if count == k.Count {
					return
				} else {
					count++
				}
			}
		}
	}()
	wg.Wait()
	return func() {
		if err := pc.Close(); err != nil {
			log.Printf("error on close partition consumer: %v", err)
		}
		if err := consumer.Close(); err != nil {
			log.Printf("error on close consumer: %v", err)
		}
	}, nil
}
