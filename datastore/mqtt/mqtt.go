package mqtt

import (
	"encoding/json"
	"fmt"

	"github.com/teslamotors/fleet-telemetry/datastore/simple/transformers"
	logrus "github.com/teslamotors/fleet-telemetry/logger"
	"github.com/teslamotors/fleet-telemetry/protos"
	"github.com/teslamotors/fleet-telemetry/telemetry"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// MessageInfo is an alias to map
type MessageInfo map[string]interface{}

type Config struct {
	// Verbose controls whether types are explicitly shown in the logs. Only applicable for record type 'V'.
	Verbose bool   `json:"verbose"`
	Server  string `json:"server"`
}

// ProtoLogger is a simple protobuf logger
type MqttProtoLogger struct {
	Config     *Config
	logger     *logrus.Logger
	mqttClient MQTT.Client
}

// NewProtoLogger initializes the parameters for protobuf payload logging
func NewMqttProtoLogger(config *Config, logger *logrus.Logger) (telemetry.Producer, error) {
	fmt.Println("Starting MQTT")
	opts := MQTT.NewClientOptions()
	opts.AddBroker(config.Server)
	opts.SetClientID("fleet-telemetry")
	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	logger.Println("MQTT Client Connected")

	return &MqttProtoLogger{Config: config, logger: logger, mqttClient: client}, nil
}

// SetReliableAckTxType no-op for logger datastore
func (p *MqttProtoLogger) ProcessReliableAck(entry *telemetry.Record) {
}

// Produce sends the data to the logger
func (p *MqttProtoLogger) Produce(entry *telemetry.Record) {
	data, err := p.recordToLogMap(entry)
	var topic string
	var msg []byte
	var err1 error
	if err != nil {
		topic = "fleet-telemetry/error"
		msg1, err := json.Marshal(MessageInfo{"error": err, "vin": entry.Vin, "metadata": entry.Metadata()})
		msg = msg1
		err1 = err
	} else {
		topic = "fleet-telemetry/v"
		// { "data": { "ChargeAmps": "0.000", "ChargeState": "Idle", "CreatedAt": "2024-10-08T20:22:14Z", "Locked": "true", "Vin": "<VIN>" } }
		msg1, err := json.Marshal(MessageInfo{"data": data})
		msg = msg1
		err1 = err
	}
	if err1 != nil {
		p.logger.ErrorLog("mqtt_producer_error", err, logrus.LogInfo{})
	} else {
		p.mqttClient.Publish(topic, 0, true, []byte(msg))
	}
}

// ReportError noop method
func (p *MqttProtoLogger) ReportError(message string, err error, logInfo logrus.LogInfo) {
}

// recordToLogMap converts the data of a record to a map or slice of maps
func (p *MqttProtoLogger) recordToLogMap(record *telemetry.Record) (interface{}, error) {
	payload, err := record.GetProtoMessage()
	if err != nil {
		return nil, err
	}

	switch payload := payload.(type) {
	case *protos.Payload:
		return transformers.PayloadToMap(payload, p.Config.Verbose, p.logger), nil
	case *protos.VehicleAlerts:
		alertMaps := make([]map[string]interface{}, len(payload.Alerts))
		for i, alert := range payload.Alerts {
			alertMaps[i] = transformers.VehicleAlertToMap(alert)
		}
		return alertMaps, nil
	case *protos.VehicleErrors:
		errorMaps := make([]map[string]interface{}, len(payload.Errors))
		for i, vehicleError := range payload.Errors {
			errorMaps[i] = transformers.VehicleErrorToMap(vehicleError)
		}
		return errorMaps, nil
	default:
		return nil, fmt.Errorf("unknown txType: %s", record.TxType)
	}
}
