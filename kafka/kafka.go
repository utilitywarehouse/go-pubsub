package kafka

import metrics "github.com/rcrowley/go-metrics"

func init() {
	metrics.UseNilMetrics = true
}
