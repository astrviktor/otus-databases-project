package internalhttp

import (
	"github.com/astrviktor/otus-databases-project/internal/prometheus"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

type StatusRecorder struct {
	http.ResponseWriter
	Status int
}

func (r *StatusRecorder) WriteHeader(status int) {
	r.Status = status
	r.ResponseWriter.WriteHeader(status)
}

func Logging(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		userAgent := r.UserAgent()

		recorder := &StatusRecorder{
			ResponseWriter: w,
			Status:         0,
		}

		h(recorder, r)

		duration := time.Since(start).Milliseconds()

		if strings.Contains(r.RequestURI, "clients") {
			prometheus.Metrics.CreateTime.Set(float64(duration))
		}

		if strings.Contains(r.RequestURI, "segment") {
			prometheus.Metrics.ResponseTime.Set(float64(duration))
		}

		log.Println(ip, r.Method, r.RequestURI, r.Proto, recorder.Status, duration, userAgent)
	}
}
