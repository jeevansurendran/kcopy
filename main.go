package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/IBM/sarama"
	"github.com/spf13/cobra"
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
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println("Failed to produce message", err)
		}
	}()
	go func() {
		defer wg.Done()
		var count int64
		fmt.Printf("Copying %d messages from %s (%s/p-%d/o-%d) to %s (%s/p-%d) \n", k.Count, strings.Join(k.Source.Addrs, ", "), k.Topic.Source, k.Partition.Source, k.Offset, strings.Join(k.Destination.Addrs, ", "), k.Topic.Destination, k.Partition.Destination)
		for message := range pc.Messages() {
			producer.Input() <- &sarama.ProducerMessage{
				Topic:     k.Topic.Destination,
				Key:       sarama.ByteEncoder(message.Key),
				Value:     sarama.ByteEncoder(message.Value),
				Partition: k.Partition.Destination,
			}
			if count == k.Count {
				break
			} else {
				count++
			}
		}
		// This usually means completed.
		producer.AsyncClose()
	}()
	wg.Wait()
	fmt.Printf("Copying completed\n")
	return func() {
		pc.Close()
		consumer.Close()
	}, nil
}

func main() {
	kcopy := NewKCopy()
	var rootCmd = &cobra.Command{
		Use:   "kcopy",
		Short: "kcopy helps you copy kafka topic messages from one kafka cluster to another. Its is particilary useful when you have binary data in kafka and you want to copy it locally for testing.",
		Run: func(cmd *cobra.Command, args []string) {
			close, err := kcopy.Copy()
			if err != nil {
				panic(err)
			}
			defer close()
		},
	}
	rootCmd.Flags().StringArrayVarP(&kcopy.Source.Addrs, "source-broker", "s", []string{}, "Set the source broker addresses.")
	rootCmd.MarkFlagRequired("source-broker")
	rootCmd.Flags().StringArrayVarP(&kcopy.Destination.Addrs, "destination-broker", "d", []string{"localhost:9092"}, "Set the destination broker addresses.")
	rootCmd.Flags().StringArrayVarP(&kcopy.Source.KeyValue, "X", "X", []string{}, "Set configuration for the source broker.")
	rootCmd.Flags().StringArrayVarP(&kcopy.Destination.KeyValue, "Y", "Y", []string{}, "Set configuration for the destination broker.")
	rootCmd.Flags().StringVarP(&kcopy.Topic.Input, "topic", "t", "", "Set the topic to copy from. To copy to a different topic, use the format source:destination.")
	rootCmd.MarkFlagRequired("topic")
	rootCmd.Flags().StringVarP(&kcopy.Partition.Input, "partition", "p", "", "Set the partition to copy from. To copy to a different partition, use the format source:destination. Defaults to 0.")
	rootCmd.Flags().Int64VarP(&kcopy.Offset, "offset", "o", -1, "Set the offset to copy from. Use -1 for the latest offset and -2 for the oldest offset. Defaults to -1.")
	rootCmd.Flags().Int64VarP(&kcopy.Count, "count", "c", 0, "Set the total number of messages to copy.")
	rootCmd.MarkFlagRequired("count")
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
