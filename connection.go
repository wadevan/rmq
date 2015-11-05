package rmq

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/adjust/redis"
	"github.com/adjust/uniuri"
)

const heartbeatDuration = time.Minute

// Connection is an interface that can be used to test publishing
type Connection interface {
	OpenQueue(name string) Queue
	CollectStats(queueList []string) Stats
	GetOpenQueues() []string
}

// Connection is the entry point. Use a connection to access queues, consumers and deliveries
// Each connection has a single heartbeat shared among all consumers
type redisConnection struct {
	Name             string
	heartbeatKey     string // key to keep alive
	queuesKey        string // key to list of queues consumed by this connection
	redisClient      *redis.Client
	heartbeatStopped bool
}

// OpenConnection opens and returns a new connection
func OpenConnection(tag, network, address string, db int) (connection *redisConnection, err error) {
	redisClient := redis.NewClient(&redis.Options{
		Network: network,
		Addr:    address,
		DB:      int64(db),
	})

	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))

	connection = &redisConnection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  redisClient,
	}

	if err := connection.updateHeartbeat(); err != nil { // checks the connection
		log.Panicf("rmq connection failed to update heartbeat %s %s", connection, err)
	}

	// add to connection set after setting heartbeat to avoid race with cleaner
	if err := redisErrNotNil(redisClient.SAdd(connectionsKey, name)); err != nil {
		return nil, err
	}

	go connection.heartbeat()
	// log.Printf("rmq connection connected to %s %s:%s %d", name, network, address, db)
	return connection, nil
}

// OpenQueue opens and returns the queue with a given name
func (connection *redisConnection) OpenQueue(name string) (queue Queue, err error) {
	if err := redisErrNotNil(connection.redisClient.SAdd(queuesKey, name)); err != nil {
		return nil, err
	}
	queue = newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
	return queue, nil
}

func (connection *redisConnection) CollectStats(queueList []string) Stats {
	return CollectStats(queueList, connection)
}

func (connection *redisConnection) String() string {
	return connection.Name
}

// GetConnections returns a list of all open connections
func (connection *redisConnection) GetConnections() (names []string, err error) {
	result := connection.redisClient.SMembers(connectionsKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return []string{}, nil
	default:
		return nil, err
	}
	return result.Val(), nil
}

// Check retuns true if the connection is currently active in terms of heartbeat
func (connection *redisConnection) Check() (ok bool, err error) {
	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, connection.Name, 1)
	result := connection.redisClient.TTL(heartbeatKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}
	return result.Val() > 0, nil
}

// StopHeartbeat stops the heartbeat of the connection
// it does not remove it from the list of connections so it can later be found by the cleaner
func (connection *redisConnection) StopHeartbeat() (ok bool, err error) {
	connection.heartbeatStopped = true
	switch err := redisErr(connection.redisClient.Del(connection.heartbeatKey)); err {
	case nil:
		return true, nil
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}
}

func (connection *redisConnection) Close() (ok bool, err error) {
	switch err := redisErr(connection.redisClient.SRem(connectionsKey, connection.Name)); err {
	case nil:
		return true, nil
	case redis.Nil:
		return false, nil
	default:
		return false, err
	}
}

// GetOpenQueues returns a list of all open queues
func (connection *redisConnection) GetOpenQueues() (names []string, err error) {
	result := connection.redisClient.SMembers(queuesKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return []string{}, nil
	default:
		return nil, err
	}
	return result.Val(), nil
}

// CloseAllQueues closes all queues by removing them from the global list
func (connection *redisConnection) CloseAllQueues() (count int, err error) {
	result := connection.redisClient.Del(queuesKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return 0, nil
	default:
		return 0, err
	}
	return int(result.Val()), nil
}

// CloseAllQueuesInConnection closes all queues in the associated connection by removing all related keys
func (connection *redisConnection) CloseAllQueuesInConnection() error {
	if err := redisErrNotNil(connection.redisClient.Del(connection.queuesKey)); err != nil {
		return err
	}
	// debug(fmt.Sprintf("connection closed all queues %s %d", connection, connection.queuesKey)) // COMMENTOUT
	return nil
}

// GetConsumingQueues returns a list of all queues consumed by this connection
func (connection *redisConnection) GetConsumingQueues() (names []string, err error) {
	result := connection.redisClient.SMembers(connection.queuesKey)
	switch err := redisErr(result); err {
	case nil:
	case redis.Nil:
		return []string{}, nil
	default:
		return nil, err
	}
	return result.Val(), nil
}

// heartbeat keeps the heartbeat key alive
func (connection *redisConnection) heartbeat() {
	for {
		if err := connection.updateHeartbeat(); err != nil {
			// TODO: return error somehow
			// log.Printf("rmq connection failed to update heartbeat %s", connection)
		}

		time.Sleep(time.Second)

		if connection.heartbeatStopped {
			// log.Printf("rmq connection stopped heartbeat %s", connection)
			return
		}
	}
}

func (connection *redisConnection) updateHeartbeat() error {
	return redisErr(connection.redisClient.SetEx(connection.heartbeatKey, heartbeatDuration, "1"))
}

// hijackConnection reopens an existing connection for inspection purposes without starting a heartbeat
func (connection *redisConnection) hijackConnection(name string) *redisConnection {
	return &redisConnection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  connection.redisClient,
	}
}

// openQueue opens a queue without adding it to the set of queues
func (connection *redisConnection) openQueue(name string) *redisQueue {
	return newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
}

// flushDb flushes the redis database to reset everything, used in tests
func (connection *redisConnection) flushDb() {
	connection.redisClient.FlushDb()
}
