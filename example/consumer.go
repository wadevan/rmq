package main

import (
	"fmt"
	"log"
	"time"

	"github.com/adjust/rmq"
)

const (
	unackedLimit = 1000
	numConsumers = 2
)

func main() {
	connection := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 2)
	queue := connection.OpenQueue("things")
	queue.StartConsuming(unackedLimit, 500*time.Millisecond)

	contexts := []*rmq.ConsumerContext{}
	for i := 0; i < numConsumers; i++ {
		name := fmt.Sprintf("consumer %d", i)
		_, context := queue.AddConsumer(name, NewConsumer(i))
		contexts = append(contexts, context)
	}

	time.Sleep(3 * time.Second)
	log.Printf("--- stopping now")
	for i, context := range contexts {
		context.StopChan <- 1
		log.Printf("--- stopped %d", i)
		before := time.Now()
		context.Wg.Wait()
		log.Printf("--- waited %d %p %s", i, context.Wg, time.Now().Sub(before))
	}
	time.Sleep(time.Second)
}

type Consumer struct {
	name      string
	count     int
	before    time.Time
	batchSize int
	nextLog   time.Time
}

func NewConsumer(tag int) *Consumer {
	return &Consumer{
		name:      fmt.Sprintf("consumer%d", tag),
		count:     0,
		before:    time.Now(),
		batchSize: 0,
		nextLog:   time.Now().Add(time.Second),
	}
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	consumer.count++
	consumer.batchSize++
	// payload := delivery.Payload()

	// log.Printf("consumer %s start %s", consumer.name, payload)
	time.Sleep(time.Second)
	// log.Printf("consumer %s done %s", consumer.name, payload)
	if consumer.count%consumer.batchSize == 0 {
		delivery.Reject()
	} else {
		delivery.Ack()
	}

	// if time.Now().After(consumer.nextLog) {
	// 	duration := time.Now().Sub(consumer.before)
	// 	consumer.before = time.Now()
	// 	perSecond := time.Second / (duration / time.Duration(consumer.batchSize))
	// 	log.Printf("%s consumed %d %s %d", consumer.name, consumer.count, payload, perSecond)
	// 	consumer.nextLog = time.Now().Add(time.Second)
	// 	consumer.batchSize = 0
	// }
}
