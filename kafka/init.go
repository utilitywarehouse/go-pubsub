package kafka

import "github.com/rcrowley/go-metrics"

func init() {
	// Disable rcrowley/go-metrics by default due to memory leak in this lib which is used by Shopify/sarama
	metrics.UseNilMetrics = true
}
