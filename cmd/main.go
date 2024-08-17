package main

import (
	"log"

	"github.com/jeevansurendran/kcopy"
	"github.com/spf13/cobra"
)

func main() {
	kcopy := kcopy.NewKCopy()
	var rootCmd = &cobra.Command{
		Use:   "kcopy",
		Short: "kcopy helps you copy kafka topic messages from one kafka cluster to another. Its is particilary useful when you have binary data in kafka and you want to copy it locally for testing.",
		Run: func(cmd *cobra.Command, args []string) {
			close, err := kcopy.Copy()
			if err != nil {
				log.Fatalf("error copying message :%v", err)
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
