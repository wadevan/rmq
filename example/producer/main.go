package main

import (
	"fmt"
	"github.com/phuslu/log"
	"time"

	"github.com/wadevan/rmq"
)

const (
	numDeliveries = 100000000
	batchSize     = 10000
)

func main() {
	connection := rmq.OpenConnection("producer", "tcp", "localhost:6379", 2)
	things := connection.OpenQueue("things")
	balls := connection.OpenQueue("balls")
	var before time.Time

	for i := 0; i < numDeliveries; i++ {
		delivery := fmt.Sprintf("delivery %d", i)
		things.Publish(delivery)
		if i%batchSize == 0 {
			duration := time.Now().Sub(before)
			before = time.Now()
			perSecond := time.Second / (duration / batchSize)
			log.Printf("produced %d %s %d", i, delivery, perSecond)
			balls.Publish("ball")
		}
	}
}
