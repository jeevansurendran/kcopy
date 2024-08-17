# KCopy - Kafka Topic Copy Utility

KCopy is a command-line utility designed to copy Kafka topic messages from one Kafka cluster to another. 
It's particularly useful when you have binary data in Kafka and want to copy it locally for testing purposes.

## Features

- Copy messages between different Kafka clusters
- Support for SASL PLAIN authentication mechanism
- Flexible topic and partition mapping
- Control over offset and message count

## Installation

To install KCopy, ensure you have Go installed on your system, then run:
```sh
go install github.com/jeevansurendran/kcopy/cmd/kcopy@latest
```
PS: Need to make sure if `$GOBIN` is configured to the `$PATH` variable

## Usage
kcopy [flags]

### Flags
- `-s, --source-broker` (required): Set the source broker addresses.
- `-d, --destination-broker` (default: "localhost:9092"): Set the destination broker addresses.
- `-X`: Set configuration for the source broker.
- `-Y`: Set configuration for the destination broker.
- `-t, --topic` (required): Set the topic to copy from. To copy to a different topic, use the format `source:destination`.
- `-p, --partition`: Set the partition to copy from. To copy to a different partition, use the format `source:destination`. Defaults to 0.
- `-o, --offset` (default: -1): Set the offset to copy from. Use -1 for the latest offset and -2 for the oldest offset.
- `-c, --count` (required): Set the total number of messages to copy.

## Example
1. Copying 100 messages from topic "source-topic" to local no auth kafka cluster.
```shell
kcopy\
-s pkc.aws.confluent.cloud:9092 \
-X tls.enable=true -X sasl.mechanism=PLAIN -X  sasl.user=username -X sasl.password=password \
-t source-topic \
-o 3309159 \
-c 100 
```
2. Copying 100 messages from one cluster to another with different topics, partitions, and cluster names.
```shell
kcopy\
-s pkc-1.aws.confluent.cloud:9092 \
-d pkc-2.aws.confluent.cloud:9092 \
-X tls.enable=true -X sasl.mechanism=PLAIN -X  sasl.user=username -X sasl.password=password \
-Y tls.enable=true -Y sasl.mechanism=PLAIN -Y  sasl.user=username -Y sasl.password=password \
-t source-topic:destination-topic \
-p 4:7 \
-o 3309159 \
-c 100 
```
