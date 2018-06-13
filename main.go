package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

const (
	keepAlive   int  = 2
	pingTimeout int  = 1
	qos1        byte = 1
	qos2        byte = 2
)

var (
	// routeMap map the [original topic] to the [new topic].
	routeMap       map[string]string
	edgeHubClient  MQTT.Client
	cloudHubClient MQTT.Client
	msgToEdgeHub   chan Msg
	msgToCloudHub  chan Msg
	stopSignal     chan os.Signal
)

var f MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("TOPIC: %s, MSG: %s\n", msg.Topic(), msg.Payload())
}

// init load the routing file and create the channels
func init() {
	setTrace(true, true)
	loadRouteFile("route.json")
	msgToEdgeHub = make(chan Msg)
	msgToCloudHub = make(chan Msg)
	stopSignal = make(chan os.Signal, 2)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)
}

// setTrace setup trace logs
func setTrace(appLog, mqttLog bool) {
	if appLog {
		// TODO: enable app logs
	}

	if mqttLog {
		MQTT.DEBUG = log.New(os.Stdout, "", 0)
		MQTT.ERROR = log.New(os.Stdout, "", 0)
	}
}

// loadRouteFile load the routing JSON file
func loadRouteFile(filename string) {
	// read route file
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("%s, %v\n", ErrLoadRouteFile, err)
		os.Exit(1)
	}
	fmt.Printf("%s\n", string(file)) // TODO: remove

	// create route map
	routeMap = make(map[string]string)

	// TODO: load the routing rules from the JSON file to the route map.
}

// createMQTTClient create a MQTT client instance
func createMQTTClient(brokerAddr, clientID, username, password string) MQTT.Client {
	opts := MQTT.NewClientOptions().AddBroker(brokerAddr)
	opts.SetClientID(clientID)
	opts.SetCleanSession(true)
	if len(username) > 0 {
		opts.SetUsername(username)
		opts.SetPassword(password)
	}
	client := MQTT.NewClient(opts)
	return client
}

// publish send a message to a MQTT broker
func publish(client MQTT.Client, msg Msg) {
	token := client.Publish(msg.Topic(), qos1, false, msg.Payload())
	if token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
}

// listenEdgeHub subscribe messages from the edge hub
func listenEdgeHub() {
	if token := edgeHubClient.Subscribe("go-mqtt/sample", qos1, f); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
}

// listenCloudHub subscribe messages from the cloud hub
func listenCloudHub() {
	// TODO
}

func main() {
	// init edge client
	edgeHubClient = createMQTTClient("tcp://iot.eclipse.org:1883", "edge", "", "")
	defer edgeHubClient.Disconnect(250)
	if token := edgeHubClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// init cloud client
	cloudHubClient = createMQTTClient("tcp://iot.eclipse.org:1883", "cloud", "", "")
	defer cloudHubClient.Disconnect(250)
	if token := cloudHubClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// subscribe
	go listenEdgeHub()
	go listenCloudHub()

	// start routing loop
	for {
		select {
		// dispatch messages to the edge MQTT broker
		case ingress := <-msgToEdgeHub:
			go publish(edgeHubClient, ingress)
		// dispatch messages to the cloud MQTT broker
		case egress := <-msgToCloudHub:
			go publish(cloudHubClient, egress)
		// graceful shutdown
		case <-stopSignal:
			fmt.Println("ready to shut down")
			// TODO: do some cleanup here
			return
		}
	}
}
