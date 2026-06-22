package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {

	data, err := json.Marshal(val)
	if err != nil {
		fmt.Printf("error marshalling data %v\n", val)
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
	if err != nil {
		fmt.Printf("error publishing to channel %v\n", err)
		return err
	}

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		fmt.Printf("error binding queue %s: %v\n", queueName, err)
		return err
	}

	deliveries, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Printf("error opening delivery channel for queue %s: %v\n", queueName, err)
	}
	go func() {
		for delivery := range deliveries {
			var data T
			err := json.Unmarshal(delivery.Body, &data)
			if err != nil {
				fmt.Printf("error unmarshalling JSON from delivery %s: %v\n", queueName, err)
			}
			handler(data)
			err = delivery.Ack(false)
			if err != nil {
				fmt.Printf("error removing item from queue: %v", err)
			}
		}
	}()
	return nil
}
