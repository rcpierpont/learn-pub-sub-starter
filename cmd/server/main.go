package main

import (
	"fmt"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rcpierpont/learn-pub-sub-starter/internal/pubsub"
	"github.com/rcpierpont/learn-pub-sub-starter/internal/routing"
)

func main() {
	connStr := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Printf("error connecting to RabbitMQ: %v\n", err)
	}
	defer conn.Close()
	fmt.Println("Connection to RabbitMQ Successful!")

	pubCh, err := conn.Channel()
	if err != nil {
		fmt.Printf("error opening channel to RabbitMQ: %v\n", err)
	}

	pubsub.PublishJSON(pubCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{})

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Interrupt signal received - exiting program.")
}
