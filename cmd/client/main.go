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
	connStr := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Client Connection to RabbitMQ Successful!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error when prompting for username: %v", err)
	}

	/*
		_, queue, err := pubsub.DeclareAndBind(
			conn,
			routing.ExchangePerilDirect,
			fmt.Sprintf("%s.%s", routing.PauseKey, username),
			routing.PauseKey,
			pubsub.QueueTypeTransient,
		)
		if err != nil {
			log.Fatalf("error subscribing to pause queue: %v", err)
		}
		fmt.Printf("Queue %v declared and bound!\n", queue.Name)
	*/
	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.QueueTypeTransient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("error subscribing to pause queue: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Printf("error executing spawn command: %v", err)
			}
		case "move":
			armyMove, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("error executing move command: %v", err)
				continue
			}
			fmt.Printf("Player %s move successful!\n", armyMove.Player.Username)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("command not recognized, try again")
			continue
		}
	}
}
