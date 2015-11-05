package rmq

import (
	"testing"
	"time"

	. "github.com/adjust/gocheck"
)

func TestCleanerSuite(t *testing.T) {
	TestingSuiteT(&CleanerSuite{}, t)
}

type CleanerSuite struct{}

func (suite *CleanerSuite) TestCleaner(c *C) {
	flushConn, _ := OpenConnection("cleaner-flush", "tcp", "localhost:6379", 1)
	flushConn.flushDb()
	flushConn.StopHeartbeat()

	conn, _ := OpenConnection("cleaner-conn1", "tcp", "localhost:6379", 1)
	names, _ := conn.GetOpenQueues()
	c.Check(names, HasLen, 0)
	q, _ := conn.OpenQueue("q1")
	queue := q.(*redisQueue)
	names, _ = conn.GetOpenQueues()
	c.Check(names, HasLen, 1)
	conn.OpenQueue("q2")
	names, _ = conn.GetOpenQueues()
	c.Check(names, HasLen, 2)

	n, _ := queue.ReadyCount()
	c.Check(n, Equals, 0)
	queue.Publish("del1")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 1)
	queue.Publish("del2")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 2)
	queue.Publish("del3")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 3)
	queue.Publish("del4")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 4)
	queue.Publish("del5")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 5)
	queue.Publish("del6")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 6)

	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 0)
	queue.StartConsuming(2, time.Millisecond)
	time.Sleep(time.Millisecond)
	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 2)
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 4)

	consumer := NewTestConsumer("c-A")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	queue.AddConsumer("consumer1", consumer)
	time.Sleep(2 * time.Millisecond)
	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 3)
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 3)

	c.Assert(consumer.LastDelivery, NotNil)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del1")
	ok, _ := consumer.LastDelivery.Ack()
	c.Check(ok, Equals, true)
	time.Sleep(2 * time.Millisecond)
	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 2)
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 3)

	consumer.Finish()
	time.Sleep(2 * time.Millisecond)
	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 3)
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 2)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del2")

	queue.StopConsuming()
	conn.StopHeartbeat()
	time.Sleep(time.Millisecond)

	conn, _ = OpenConnection("cleaner-conn1", "tcp", "localhost:6379", 1)
	q, _ = conn.OpenQueue("q1")
	queue = q.(*redisQueue)

	queue.Publish("del7")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 3)
	queue.Publish("del7")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 4)
	queue.Publish("del8")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 5)
	queue.Publish("del9")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 6)
	queue.Publish("del10")
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 7)

	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 0)
	queue.StartConsuming(2, time.Millisecond)
	time.Sleep(time.Millisecond)
	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 2)
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 5)

	consumer = NewTestConsumer("c-B")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	queue.AddConsumer("consumer2", consumer)
	time.Sleep(2 * time.Millisecond)
	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 3)
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 4)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del5")

	consumer.Finish() // unacked
	time.Sleep(2 * time.Millisecond)
	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 4)
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 3)

	c.Check(consumer.LastDelivery.Payload(), Equals, "del6")
	ok, _ = consumer.LastDelivery.Ack()
	c.Check(ok, Equals, true)
	time.Sleep(2 * time.Millisecond)
	n, _ = queue.UnackedCount()
	c.Check(n, Equals, 3)
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 3)

	queue.StopConsuming()
	conn.StopHeartbeat()
	time.Sleep(time.Millisecond)

	cleanerConn, _ := OpenConnection("cleaner-conn", "tcp", "localhost:6379", 1)
	cleaner := NewCleaner(cleanerConn)
	c.Check(cleaner.Clean(), IsNil)
	n, _ = queue.ReadyCount()
	c.Check(n, Equals, 9) // 2 of 11 were acked above
	names, _ = conn.GetOpenQueues()
	c.Check(names, HasLen, 2)

	conn, _ = OpenConnection("cleaner-conn1", "tcp", "localhost:6379", 1)
	q, _ = conn.OpenQueue("q1")
	queue = q.(*redisQueue)
	queue.StartConsuming(10, time.Millisecond)
	consumer = NewTestConsumer("c-C")

	queue.AddConsumer("consumer3", consumer)
	time.Sleep(10 * time.Millisecond)
	c.Check(consumer.LastDeliveries, HasLen, 9)

	queue.StopConsuming()
	conn.StopHeartbeat()
	time.Sleep(time.Millisecond)

	c.Check(cleaner.Clean(), IsNil)
	cleanerConn.StopHeartbeat()
}
