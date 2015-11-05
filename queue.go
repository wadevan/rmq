package rmq

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/adjust/redis"
	"github.com/adjust/uniuri"
)

const (
	connectionsKey                   = "rmq::connections"                                           // Set of connection names
	connectionHeartbeatTemplate      = "rmq::connection::{connection}::heartbeat"                   // expires after {connection} died
	connectionQueuesTemplate         = "rmq::connection::{connection}::queues"                      // Set of queues consumers of {connection} are consuming
	connectionQueueConsumersTemplate = "rmq::connection::{connection}::queue::[{queue}]::consumers" // Set of all consumers from {connection} consuming from {queue}
	connectionQueueUnackedTemplate   = "rmq::connection::{connection}::queue::[{queue}]::unacked"   // List of deliveries consumers of {connection} are currently consuming

	queuesKey             = "rmq::queues"                     // Set of all open queues
	queueReadyTemplate    = "rmq::queue::[{queue}]::ready"    // List of deliveries in that {queue} (right is first and oldest, left is last and youngest)
	queueRejectedTemplate = "rmq::queue::[{queue}]::rejected" // List of rejected deliveries from that {queue}

	phConnection = "{connection}" // connection name
	phQueue      = "{queue}"      // queue name
	phConsumer   = "{consumer}"   // consumer name (consisting of tag and token)
)

type Queue interface {
	Publish(payload string) (ok bool, err error)
	PublishBytes(payload []byte) (ok bool, err error)
	SetPushQueue(pushQueue Queue)
	StartConsuming(prefetchLimit int, pollDuration time.Duration) (ok bool, err error)
	StopConsuming() bool
	AddConsumer(tag string, consumer Consumer) (name string, err error)
	AddBatchConsumer(tag string, batchSize int, consumer BatchConsumer) (name string, err error)
	PurgeReady() (ok bool, err error)
	PurgeRejected() (ok bool, err error)
	ReturnRejected(count int) (n int, err error)
	ReturnAllRejected() (count int, err error)
	Close() (ok bool, err error)
}

type redisQueue struct {
	name             string
	connectionName   string
	queuesKey        string // key to list of queues consumed by this connection
	consumersKey     string // key to set of consumers using this connection
	readyKey         string // key to list of ready deliveries
	rejectedKey      string // key to list of rejected deliveries
	unackedKey       string // key to list of currently consuming deliveries
	pushKey          string // key to list of pushed deliveries
	redisClient      *redis.Client
	deliveryChan     chan Delivery // nil for publish channels, not nil for consuming channels
	prefetchLimit    int           // max number of prefetched deliveries number of unacked can go up to prefetchLimit + numConsumers
	pollDuration     time.Duration
	consumingStopped bool
}

func newQueue(name, connectionName, queuesKey string, redisClient *redis.Client) *redisQueue {
	consumersKey := strings.Replace(connectionQueueConsumersTemplate, phConnection, connectionName, 1)
	consumersKey = strings.Replace(consumersKey, phQueue, name, 1)

	readyKey := strings.Replace(queueReadyTemplate, phQueue, name, 1)
	rejectedKey := strings.Replace(queueRejectedTemplate, phQueue, name, 1)

	unackedKey := strings.Replace(connectionQueueUnackedTemplate, phConnection, connectionName, 1)
	unackedKey = strings.Replace(unackedKey, phQueue, name, 1)

	queue := &redisQueue{
		name:           name,
		connectionName: connectionName,
		queuesKey:      queuesKey,
		consumersKey:   consumersKey,
		readyKey:       readyKey,
		rejectedKey:    rejectedKey,
		unackedKey:     unackedKey,
		redisClient:    redisClient,
	}
	return queue
}

func (queue *redisQueue) String() string {
	return fmt.Sprintf("[%s conn:%s]", queue.name, queue.connectionName)
}

// Publish adds a delivery with the given payload to the queue
func (queue *redisQueue) Publish(payload string) (ok bool, err error) {
	// debug(fmt.Sprintf("publish %s %s", payload, queue)) // COMMENTOUT
	switch err := redisErr(queue.redisClient.LPush(queue.readyKey, payload)); err {
	case nil:
		return true, nil
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}
}

// PublishBytes just casts the bytes and calls Publish
func (queue *redisQueue) PublishBytes(payload []byte) (ok bool, err error) {
	return queue.Publish(string(payload))
}

// PurgeReady removes all ready deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeReady() (ok bool, err error) {
	result := queue.redisClient.Del(queue.readyKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}
	return result.Val() > 0, nil
}

// PurgeRejected removes all rejected deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeRejected() (ok bool, err error) {
	result := queue.redisClient.Del(queue.rejectedKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}
	return result.Val() > 0, nil
}

// Close purges and removes the queue from the list of queues
func (queue *redisQueue) Close() (ok bool, err error) {
	queue.PurgeRejected()
	queue.PurgeReady()
	result := queue.redisClient.SRem(queuesKey, queue.name)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}
	return result.Val() > 0, nil
}

func (queue *redisQueue) ReadyCount() (count int, err error) {
	result := queue.redisClient.LLen(queue.readyKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return 0, nil
	default:
		return 0, err
	}
	return int(result.Val()), nil
}

func (queue *redisQueue) UnackedCount() (count int, err error) {
	result := queue.redisClient.LLen(queue.unackedKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return 0, nil
	default:
		return 0, err
	}
	return int(result.Val()), nil
}

func (queue *redisQueue) RejectedCount() (count int, err error) {
	result := queue.redisClient.LLen(queue.rejectedKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return 0, nil
	default:
		return 0, err
	}
	return int(result.Val()), nil
}

// ReturnAllUnacked moves all unacked deliveries back to the ready
// queue and deletes the unacked key afterwards, returns number of returned
// deliveries
func (queue *redisQueue) ReturnAllUnacked() (count int, err error) {
	result := queue.redisClient.LLen(queue.unackedKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return 0, nil
	default:
		return 0, err
	}

	unackedCount := int(result.Val())
	for i := 0; i < unackedCount; i++ {
		switch err = redisErr(queue.redisClient.RPopLPush(queue.unackedKey, queue.readyKey)); err {
		case nil:
		case redis.Nil:
			return i, nil
		default:
			return 0, err
		}
		// debug(fmt.Sprintf("rmq queue returned unacked delivery %s %s", result.Val(), queue.readyKey)) // COMMENTOUT
	}

	return unackedCount, nil
}

// ReturnAllRejected moves all rejected deliveries back to the ready
// list and returns the number of returned deliveries
func (queue *redisQueue) ReturnAllRejected() (count int, err error) {
	result := queue.redisClient.LLen(queue.rejectedKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return 0, nil
	default:
		return 0, err
	}

	rejectedCount := int(result.Val())
	return queue.ReturnRejected(rejectedCount)
}

// ReturnRejected tries to return count rejected deliveries back to
// the ready list and returns the number of returned deliveries
func (queue *redisQueue) ReturnRejected(count int) (n int, err error) {
	if count == 0 {
		return 0, nil
	}

	for i := 0; i < count; i++ {
		result := queue.redisClient.RPopLPush(queue.rejectedKey, queue.readyKey)
		switch err := redisErr(result); err {
		case nil:
		case redis.Nil:
			return i, nil
		default:
			return 0, err
		}
		// debug(fmt.Sprintf("rmq queue returned rejected delivery %s %s", result.Val(), queue.readyKey)) // COMMENTOUT
	}

	return count, nil
}

// CloseInConnection closes the queue in the associated connection by removing all related keys
func (queue *redisQueue) CloseInConnection() error {
	if err := redisErrNotNil(queue.redisClient.Del(queue.unackedKey)); err != nil {
		return err
	}
	if err := redisErrNotNil(queue.redisClient.Del(queue.consumersKey)); err != nil {
		return err
	}
	return redisErr(queue.redisClient.SRem(queue.queuesKey, queue.name))
}

func (queue *redisQueue) SetPushQueue(pushQueue Queue) {
	redisPushQueue, ok := pushQueue.(*redisQueue)
	if !ok {
		return
	}

	queue.pushKey = redisPushQueue.readyKey
}

// StartConsuming starts consuming into a channel of size prefetchLimit
// must be called before consumers can be added!
// pollDuration is the duration the queue sleeps before checking for new deliveries
func (queue *redisQueue) StartConsuming(prefetchLimit int, pollDuration time.Duration) (ok bool, err error) {
	if queue.deliveryChan != nil {
		return false, nil // already consuming
	}

	// add queue to list of queues consumed on this connection
	switch err := redisErr(queue.redisClient.SAdd(queue.queuesKey, queue.name)); err {
	case nil:
	case redis.Nil:
		log.Panicf("rmq queue failed to start consuming %s", queue)
	default:
		return false, err
	}

	queue.prefetchLimit = prefetchLimit
	queue.pollDuration = pollDuration
	queue.deliveryChan = make(chan Delivery, prefetchLimit)
	// log.Printf("rmq queue started consuming %s %d %s", queue, prefetchLimit, pollDuration)
	go queue.consume()
	return true, nil
}

func (queue *redisQueue) StopConsuming() bool {
	if queue.deliveryChan == nil || queue.consumingStopped {
		return false // not consuming or already stopped
	}

	queue.consumingStopped = true
	return true
}

// AddConsumer adds a consumer to the queue and returns its internal name
// panics if StartConsuming wasn't called before!
func (queue *redisQueue) AddConsumer(tag string, consumer Consumer) (name string, err error) {
	name, err = queue.addConsumer(tag)
	if err != nil {
		return "", err
	}
	go queue.consumerConsume(consumer)
	return name, nil
}

// AddBatchConsumer is similar to AddConsumer, but for batches of deliveries
func (queue *redisQueue) AddBatchConsumer(tag string, batchSize int, consumer BatchConsumer) (name string, err error) {
	name, err = queue.addConsumer(tag)
	if err != nil {
		return "", err
	}
	go queue.consumerBatchConsume(batchSize, consumer)
	return name, nil
}

func (queue *redisQueue) GetConsumers() (names []string, err error) {
	result := queue.redisClient.SMembers(queue.consumersKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return []string{}, nil
	default:
		return nil, err
	}
	return result.Val(), nil
}

func (queue *redisQueue) RemoveConsumer(name string) (ok bool, err error) {
	result := queue.redisClient.SRem(queue.consumersKey, name)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}
	return result.Val() > 0, nil
}

func (queue *redisQueue) addConsumer(tag string) (name string, err error) {
	if queue.deliveryChan == nil {
		log.Panicf("rmq queue failed to add consumer, call StartConsuming first! %s", queue)
	}

	name = fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))

	// add consumer to list of consumers of this queue
	switch err := redisErr(queue.redisClient.SAdd(queue.consumersKey, name)); err {
	case nil:
	case redis.Nil:
		log.Panicf("rmq queue failed to add consumer %s %s", queue, tag)
	default:
		return "", err
	}

	// log.Printf("rmq queue added consumer %s %s", queue, name)
	return name, nil
}

func (queue *redisQueue) RemoveAllConsumers() (count int, err error) {
	result := queue.redisClient.Del(queue.consumersKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return 0, nil
	default:
		return 0, err
	}
	return int(result.Val()), nil
}

func (queue *redisQueue) consume() {
	for {
		batchSize, err := queue.batchSize()
		if err != nil {
			// TODO: return error somehow
		}
		wantMore, err := queue.consumeBatch(batchSize)
		if err != nil {
			// TODO: return error somehow
		}

		if !wantMore {
			time.Sleep(queue.pollDuration)
		}

		if queue.consumingStopped {
			// log.Printf("rmq queue stopped consuming %s", queue)
			return
		}
	}
}

func (queue *redisQueue) batchSize() (n int, err error) {
	prefetchCount := len(queue.deliveryChan)
	prefetchLimit := queue.prefetchLimit - prefetchCount
	// TODO: ignore ready count here and just return prefetchLimit?
	readyCount, err := queue.ReadyCount()
	if err != nil {
		return 0, err
	}
	if readyCount < prefetchLimit {
		return readyCount, nil
	}
	return prefetchLimit, nil
}

// consumeBatch tries to read batchSize deliveries, returns true if any and all were consumed
func (queue *redisQueue) consumeBatch(batchSize int) (ok bool, err error) {
	if batchSize == 0 {
		return false, nil
	}

	for i := 0; i < batchSize; i++ {
		result := queue.redisClient.RPopLPush(queue.readyKey, queue.unackedKey)
		switch err := redisErr(result); err {
		case nil:
		case redis.Nil:
			// debug(fmt.Sprintf("rmq queue consumed last batch %s %d", queue, i)) // COMMENTOUT
			return false, nil
		default:
			return false, err
		}

		// debug(fmt.Sprintf("consume %d/%d %s %s", i, batchSize, result.Val(), queue)) // COMMENTOUT
		queue.deliveryChan <- newDelivery(result.Val(), queue.unackedKey, queue.rejectedKey, queue.pushKey, queue.redisClient)
	}

	// debug(fmt.Sprintf("rmq queue consumed batch %s %d", queue, batchSize)) // COMMENTOUT
	return true, nil
}

func (queue *redisQueue) consumerConsume(consumer Consumer) {
	for delivery := range queue.deliveryChan {
		// debug(fmt.Sprintf("consumer consume %s %s", delivery, consumer)) // COMMENTOUT
		consumer.Consume(delivery)
	}
}

func (queue *redisQueue) consumerBatchConsume(batchSize int, consumer BatchConsumer) {
	batch := []Delivery{}
	waitUntil := time.Now().UTC().Add(time.Second)

	for delivery := range queue.deliveryChan {
		batch = append(batch, delivery)
		now := time.Now().UTC()
		// debug(fmt.Sprintf("batch consume added delivery %d", len(batch))) // COMMENTOUT

		if len(batch) < batchSize && now.Before(waitUntil) {
			// debug(fmt.Sprintf("batch consume wait %d < %d", len(batch), batchSize)) // COMMENTOUT
			continue
		}

		// debug(fmt.Sprintf("batch consume consume %d", len(batch))) // COMMENTOUT
		consumer.Consume(batch)

		batch = []Delivery{}
		waitUntil = time.Now().UTC().Add(time.Second)
	}
}

// // redisErrIsNil returns false if there is no error, true if the result error is nil and panics if there's another error
// func redisErrIsNil(result redis.Cmder) bool {
// 	switch result.Err() {
// 	case nil:
// 		return false
// 	case redis.Nil:
// 		return true
// 	default:
// 		log.Panicf("rmq redis error is not nil %s", result.Err())
// 		return false
// 	}
// }

func panicErr(err error) {
	if err != nil {
		log.Panic(err)
	}
}

// return the error, but wrap non nil errors
func redisErr(result redis.Cmder) error {
	switch err := result.Err(); err {
	case nil, redis.Nil:
		return err
	default:
		return fmt.Errorf("rmq redis error is not nil %s", err)
	}
}

// only return real redis errors as error
func redisErrNotNil(result redis.Cmder) error {
	switch err := result.Err(); err {
	case nil, redis.Nil:
		return nil
	default:
		return fmt.Errorf("rmq redis error is not nil %s", err)
	}
}

func debug(message string) {
	// log.Printf("rmq debug: %s", message) // COMMENTOUT
}
