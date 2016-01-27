package rmq

import "sync"

type ConsumerContext struct {
	StopChan chan int
	Wg       *sync.WaitGroup
}

func NewConsumerContext() *ConsumerContext {
	return &ConsumerContext{
		StopChan: make(chan int, 1),
		Wg:       &sync.WaitGroup{},
	}
}
