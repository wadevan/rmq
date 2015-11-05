package rmq

import "time"

type TestQueue struct {
	name           string
	LastDeliveries []string
}

func NewTestQueue(name string) *TestQueue {
	queue := &TestQueue{name: name}
	queue.Reset()
	return queue
}

func (queue *TestQueue) String() (name string, err error) {
	return queue.name, nil
}

func (queue *TestQueue) Publish(payload string) (ok bool, err error) {
	queue.LastDeliveries = append(queue.LastDeliveries, payload)
	return true, nil
}

func (queue *TestQueue) PublishBytes(payload []byte) (ok bool, err error) {
	return queue.Publish(string(payload))
}

func (queue *TestQueue) SetPushQueue(pushQueue Queue) {
}

func (queue *TestQueue) StartConsuming(prefetchLimit int, pollDuration time.Duration) (ok bool, err error) {
	return true, nil
}

func (queue *TestQueue) StopConsuming() bool {
	return true
}

func (queue *TestQueue) AddConsumer(tag string, consumer Consumer) (name string, err error) {
	return "", nil
}

func (queue *TestQueue) AddBatchConsumer(tag string, batchSize int, consumer BatchConsumer) (name string, err error) {
	return "", nil
}

func (queue *TestQueue) ReturnRejected(count int) (n int, err error) {
	return 0, nil
}

func (queue *TestQueue) ReturnAllRejected() (n int, err error) {
	return 0, nil
}

func (queue *TestQueue) PurgeReady() (ok bool, err error) {
	return false, nil
}

func (queue *TestQueue) PurgeRejected() (ok bool, err error) {
	return false, nil
}

func (queue *TestQueue) Close() (ok bool, err error) {
	return false, nil
}

func (queue *TestQueue) Reset() {
	queue.LastDeliveries = []string{}
}
