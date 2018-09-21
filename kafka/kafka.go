package kafka

import metrics "github.com/rcrowley/go-metrics"

// InternalMetrics enables or disables sarama internal metrics
func InternalMetrics(enabled bool) {
	metrics.UseNilMetrics = !enabled
}
