package rmq

import (
	"fmt"
	"github.com/adjust/redis"
)

type Delivery interface {
	Payload() string
	Ack() (ok bool, err error)
	Reject() (ok bool, err error)
	Push() (ok bool, err error)
}

type wrapDelivery struct {
	payload     string
	unackedKey  string
	rejectedKey string
	pushKey     string
	redisClient *redis.Client
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

func (delivery *wrapDelivery) String() string {
	return fmt.Sprintf("[%s %s]", delivery.payload, delivery.unackedKey)
}

func (delivery *wrapDelivery) Payload() string {
	return delivery.payload
}

func (delivery *wrapDelivery) Ack() (ok bool, err error) {
	// debug(fmt.Sprintf("delivery ack %s", delivery)) // COMMENTOUT

	result := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}

	return result.Val() == 1, nil
}

func (delivery *wrapDelivery) Reject() (ok bool, err error) {
	return delivery.move(delivery.rejectedKey)
}

func (delivery *wrapDelivery) Push() (ok bool, err error) {
	if delivery.pushKey != "" {
		return delivery.move(delivery.pushKey)
	} else {
		return delivery.move(delivery.rejectedKey)
	}
}

func (delivery *wrapDelivery) move(key string) (ok bool, err error) {
	switch err = redisErr(delivery.redisClient.LPush(key, delivery.payload)); err {
	case nil:
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}

	switch err := redisErr(delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)); err {
	case nil:
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}

	// debug(fmt.Sprintf("delivery rejected %s", delivery)) // COMMENTOUT
	return true, nil
}
