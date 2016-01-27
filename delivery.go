package rmq

import (
	"fmt"
	"log"
	"sync"

	"gopkg.in/redis.v3"
)

type Delivery interface {
	Payload() string
	Ack() bool
	Reject() bool
	Push() bool
	setWg(wg *sync.WaitGroup)
}

type wrapDelivery struct {
	payload     string
	unackedKey  string
	rejectedKey string
	pushKey     string
	redisClient *redis.Client
	wg          *sync.WaitGroup
}

func newDelivery(payload, unackedKey, rejectedKey, pushKey string, redisClient *redis.Client) *wrapDelivery {
	return &wrapDelivery{
		payload:     payload,
		unackedKey:  unackedKey,
		rejectedKey: rejectedKey,
		pushKey:     pushKey,
		redisClient: redisClient,
	}
}

func (delivery *wrapDelivery) setWg(wg *sync.WaitGroup) {
	delivery.wg = wg
}

func (delivery *wrapDelivery) String() string {
	return fmt.Sprintf("[%s %s]", delivery.payload, delivery.unackedKey)
}

func (delivery *wrapDelivery) Payload() string {
	return delivery.payload
}

func (delivery *wrapDelivery) Ack() bool {
	defer func() {
		delivery.wg.Done()
		log.Printf("wg done %p", delivery.wg)
	}()
	// debug(fmt.Sprintf("delivery ack %s", delivery)) // COMMENTOUT

	result := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	if redisErrIsNil(result) {
		return false
	}

	return result.Val() == 1
}

func (delivery *wrapDelivery) Reject() bool {
	defer func() {
		delivery.wg.Done()
		log.Printf("wg done %p", delivery.wg)
	}()
	return delivery.move(delivery.rejectedKey)
}

func (delivery *wrapDelivery) Push() bool {
	defer func() {
		delivery.wg.Done()
		log.Printf("wg done %p", delivery.wg)
	}()
	if delivery.pushKey != "" {
		return delivery.move(delivery.pushKey)
	} else {
		return delivery.move(delivery.rejectedKey)
	}
}

func (delivery *wrapDelivery) move(key string) bool {
	if redisErrIsNil(delivery.redisClient.LPush(key, delivery.payload)) {
		return false
	}

	if redisErrIsNil(delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)) {
		return false
	}

	// debug(fmt.Sprintf("delivery rejected %s", delivery)) // COMMENTOUT
	return true
}
