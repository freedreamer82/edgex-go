//
// Copyright (c) 2017
// Cavium
// Mainflux
// IOTech
//
// SPDX-License-Identifier: Apache-2.0
//

package distro

import (
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/edgexfoundry/edgex-go/internal/pkg/correlation/models"
	contract "github.com/edgexfoundry/go-mod-core-contracts/models"
)
//Mqtt options config
type MqttConfig struct {
	qos    		  byte
	retain 		  bool
	autoreconnect bool
}

type mqttSender struct {
	client MQTT.Client
	topic  string
	opts   MqttConfig
}


// default mqttConfig
func newMqttConfig() *MqttConfig {
	mqttConfig := new(MqttConfig)
	mqttConfig.qos 	         = 0
	mqttConfig.retain        = false
	mqttConfig.autoreconnect = false

	return mqttConfig
}

// newMqttSender - create new mqtt sender
func newMqttSender(addr contract.Addressable, cert string, key string , config *MqttConfig ) sender {
	protocol := strings.ToLower(addr.Protocol)

	opts := MQTT.NewClientOptions()
	broker := protocol + "://" + addr.Address + ":" + strconv.Itoa(addr.Port) + addr.Path
	opts.AddBroker(broker)
	opts.SetClientID(addr.Publisher)
	opts.SetUsername(addr.User)
	opts.SetPassword(addr.Password)
	opts.SetAutoReconnect(config.autoreconnect)


	if protocol == "tcps" || protocol == "ssl" || protocol == "tls" {
		cert, err := tls.LoadX509KeyPair(cert, key)

		if err != nil {
			LoggingClient.Error("Failed loading x509 data")
			return nil
		}

		tlsConfig := &tls.Config{
			ClientCAs:          nil,
			InsecureSkipVerify: true,
			Certificates:       []tls.Certificate{cert},
		}

		opts.SetTLSConfig(tlsConfig)

	}

	sender := &mqttSender{
		client: MQTT.NewClient(opts),
		topic:  addr.Topic,
		opts:   *config,
	}

	return sender
}

func (sender *mqttSender) Send(data []byte, event *models.Event) bool {
	if !sender.client.IsConnected() {
		LoggingClient.Info("Connecting to mqtt server")
		if token := sender.client.Connect(); token.Wait() && token.Error() != nil {
			LoggingClient.Error(fmt.Sprintf("Could not connect to mqtt server, drop event. Error: %s", token.Error().Error()))
			return false
		}
	}

	token := sender.client.Publish(sender.topic, sender.opts.qos, sender.opts.retain, data)
	// FIXME: could be removed? set of tokens?
	token.Wait()
	if token.Error() != nil {
		LoggingClient.Error(token.Error().Error())
		return false
	} else {
		LoggingClient.Debug(fmt.Sprintf("Sent data: %X", data))
		return true
	}
}
