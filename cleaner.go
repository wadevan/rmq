package rmq

import "fmt"

type Cleaner struct {
	connection *redisConnection
}

func NewCleaner(connection *redisConnection) *Cleaner {
	return &Cleaner{connection: connection}
}

func (cleaner *Cleaner) Clean() error {
	connectionNames, err := cleaner.connection.GetConnections()
	panicErr(err)
	for _, connectionName := range connectionNames {
		connection := cleaner.connection.hijackConnection(connectionName)
		active, err := connection.Check()
		panicErr(err)
		if active {
			continue // skip active connections!
		}

		if err := cleaner.CleanConnection(connection); err != nil {
			return err
		}
	}

	return nil
}

func (cleaner *Cleaner) CleanConnection(connection *redisConnection) error {
	queueNames, err := connection.GetConsumingQueues()
	panicErr(err)
	for _, queueName := range queueNames {
		queue, err := connection.OpenQueue(queueName)
		panicErr(err)
		rQueue, ok := queue.(*redisQueue)
		if !ok {
			return fmt.Errorf("rmq cleaner failed to open queue %s", queueName)
		}

		cleaner.CleanQueue(rQueue)
	}

	ok, err := connection.Close()
	panicErr(err)
	if !ok {
		return fmt.Errorf("rmq cleaner failed to close connection %s", connection)
	}

	if err := connection.CloseAllQueuesInConnection(); err != nil {
		return fmt.Errorf("rmq cleaner failed to close all queues %d %s", connection, err)
	}

	// log.Printf("rmq cleaner cleaned connection %s", connection)
	return nil
}

func (cleaner *Cleaner) CleanQueue(queue *redisQueue) {
	returned, err := queue.ReturnAllUnacked()
	panicErr(err)
	panicErr(queue.CloseInConnection())
	_ = returned
	// log.Printf("rmq cleaner cleaned queue %s %d", queue, returned)
}
