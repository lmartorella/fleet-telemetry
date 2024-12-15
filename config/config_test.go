package config

import (
	"io"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	githublogrus "github.com/sirupsen/logrus"

	logrus "github.com/teslamotors/fleet-telemetry/logger"
	"github.com/teslamotors/fleet-telemetry/metrics"
	"github.com/teslamotors/fleet-telemetry/server/airbrake"
	"github.com/teslamotors/fleet-telemetry/telemetry"
)

var _ = Describe("Test full application config", func() {

	var (
		config    *Config
		producers map[string][]telemetry.Producer
		log       *logrus.Logger
	)

	BeforeEach(func() {
		log, _ = logrus.NoOpLogger()
		config = &Config{
			Host:       "127.0.0.1",
			Port:       443,
			StatusPort: 8080,
			Namespace:  "tesla_telemetry",
			TLS:        &TLS{CAFile: "tesla.ca", ServerCert: "your_own_cert.crt", ServerKey: "your_own_key.key"},
			RateLimit:  &RateLimit{Enabled: true, MessageLimit: 1000, MessageInterval: 30},
			Monitoring:    &metrics.MonitoringConfig{PrometheusMetricsPort: 9090, ProfilerPort: 4269, ProfilingPath: "/tmp/fleet-telemetry/profile/"},
			LogLevel:      "info",
			JSONLogEnable: true,
			Records:       map[string][]telemetry.Dispatcher{"V": {"kafka"}},
		}
	})

	AfterEach(func() {
		os.Clearenv()
		type Closer interface {
			Close() error
		}
		for _, typeProducers := range producers {
			for _, producer := range typeProducers {
				if closer, ok := producer.(Closer); ok {
					err := closer.Close()
					Expect(err).NotTo(HaveOccurred())
				}
			}
		}
	})

	Context("ExtractServiceTLSConfig", func() {
		It("fails when TLS is nil ", func() {
			config = &Config{}
			_, err := config.ExtractServiceTLSConfig(log)
			Expect(err).To(MatchError("tls config is empty - telemetry server is mTLS only, make sure to provide certificates in the config"))
		})

		It("fails when files are missing", func() {
			_, err := config.ExtractServiceTLSConfig(log)
			Expect(err).To(MatchError("open tesla.ca: no such file or directory"))
		})

		It("fails when pem file is invalid", func() {
			tmpCA, err := os.CreateTemp(GinkgoT().TempDir(), "tmpCA")
			Expect(err).NotTo(HaveOccurred())

			_, err = io.WriteString(tmpCA, "-----BEGIN CERTIFICATE-----\nFAKECA\n-----END CERTIFICATE-----")
			Expect(err).NotTo(HaveOccurred())
			config.TLS.CAFile = tmpCA.Name()

			_, err = config.ExtractServiceTLSConfig(log)
			Expect(err).To(MatchError(MatchRegexp("custom ca not properly loaded: .*tmpCA.*")))
		})

		It("uses prod CA", func() {
			config.TLS.CAFile = ""

			tls, err := config.ExtractServiceTLSConfig(log)
			Expect(err).NotTo(HaveOccurred())
			Expect(tls).NotTo(BeNil())
			Expect(tls.ClientCAs).NotTo(BeNil())
			Expect(tls.ClientCAs.Subjects()).To(HaveLen(14)) //nolint:staticcheck
		})

		It("uses eng CA", func() {
			config.TLS.CAFile = ""
			config.UseDefaultEngCA = true

			tls, err := config.ExtractServiceTLSConfig(log)
			Expect(err).NotTo(HaveOccurred())
			Expect(tls).NotTo(BeNil())
			Expect(tls.ClientCAs).NotTo(BeNil())
			Expect(tls.ClientCAs.Subjects()).To(HaveLen(8)) //nolint:staticcheck
		})
	})

	Context("basic config", func() {
		It("use correct ports", func() {
			config, err := loadTestApplicationConfig(TestSmallConfig)
			Expect(err).NotTo(HaveOccurred())
			Expect(config.Port).To(BeEquivalentTo(443))
			Expect(config.StatusPort).To(BeEquivalentTo(8080))
		})

		It("transmitrecords disabled by default", func() {
			config, err := loadTestApplicationConfig(TestSmallConfig)
			Expect(err).NotTo(HaveOccurred())
			Expect(config.TransmitDecodedRecords).To(BeFalse())
		})

		It("transmitrecords enabled", func() {
			config, err := loadTestApplicationConfig(TestTransmitDecodedRecords)
			Expect(err).NotTo(HaveOccurred())
			Expect(config.TransmitDecodedRecords).To(BeTrue())
		})
	})

	Context("configure reliable acks", func() {

		DescribeTable("fails",
			func(configInput string, errMessage string) {

				config, err := loadTestApplicationConfig(configInput)
				Expect(err).NotTo(HaveOccurred())

				producers, err = config.ConfigureProducers(airbrake.NewAirbrakeHandler(nil), log)
				Expect(err).To(MatchError(errMessage))
				Expect(producers).To(BeNil())
			},
			Entry("when reliable ack is mapped incorrectly", TestBadReliableAckConfig, "pubsub cannot be configured as reliable ack for record: V. Valid datastores configured [kafka]"),
			Entry("when logger is configured as reliable ack", TestLoggerAsReliableAckConfig, "logger cannot be configured as reliable ack for record: V"),
			Entry("when reliable ack is configured for unmapped txtype", TestUnusedTxTypeAsReliableAckConfig, "kafka cannot be configured as reliable ack for record: error since no record mapping exists"),
		)

	})

	Context("configureMetricsCollector", func() {
		It("does not fail when TLS is nil ", func() {
			log, _ := logrus.NoOpLogger()
			config = &Config{}
			config.configureMetricsCollector(log)

			Expect(config.Monitoring).To(BeNil())
		})

		It("fails if not reachable", func() {
			log, _ := logrus.NoOpLogger()
			config.configureMetricsCollector(log)
			Expect(config.MetricCollector).NotTo(BeNil())
		})
	})

	Context("configureLogger", func() {
		It("Should properly configure logger", func() {
			log, _ := logrus.NoOpLogger()
			config.configureLogger(log)

			Expect(githublogrus.GetLevel().String()).To(Equal("info"))
		})
	})
})
