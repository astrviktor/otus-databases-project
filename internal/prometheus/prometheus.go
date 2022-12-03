package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
)

type MetricsStruct struct {
	CreateTime   prometheus.Gauge
	ResponseTime prometheus.Gauge
}

func NewPrometheus() {
	createTime := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "create_time",
			Help: "create time, ms",
		})

	responseTime := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "response_time",
			Help: "response time, ms",
		})

	prometheus.MustRegister(createTime)
	prometheus.MustRegister(responseTime)

	createTime.Set(0)
	responseTime.Set(0)

	Metrics.CreateTime = createTime
	Metrics.ResponseTime = responseTime
}

var Metrics MetricsStruct
