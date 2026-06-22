package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const connStr = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Connection to RabbitMQ Successful!")

	pubCh, err := conn.Channel()
	if err != nil {
		fmt.Printf("error opening channel to RabbitMQ: %v\n", err)
	}

	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.QueueTypeDurable,
	)
	if err != nil {
		log.Fatalf("error subscribing to pause queue: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		if words[0] == "pause" {
			fmt.Println("sending pause message...")
			err = pubsub.PublishJSON(
				pubCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true},
			)
			if err != nil {
				log.Printf("could not send pause message: %v", err)
				continue
			}
			fmt.Println("Pause message sent!")
		} else if words[0] == "resume" {
			fmt.Println("sending pause message...")
			err = pubsub.PublishJSON(
				pubCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false},
			)
			if err != nil {
				log.Printf("could not send pause message: %v", err)
				continue
			}
			fmt.Println("Pause message sent!")
		} else if words[0] == "quit" {
			fmt.Println("exiting...")
			break
		} else {
			fmt.Println("unrecognized command, please review server help and try again")
		}
	}

}
