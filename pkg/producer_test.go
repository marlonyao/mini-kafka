package mini_kafka

import (
	"testing"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

// ─── 分区策略测试 ──────────────────────────────────

func TestRoundRobinPartitioner(t *testing.T) {
	p := NewRoundRobinPartitioner()
	numPart := 3

	results := make([]int, 9)
	for i := 0; i < 9; i++ {
		results[i] = p.Partition(message.NewRecordWithString("", "msg"), numPart)
	}

	// 应该是 0,1,2,0,1,2,0,1,2
	expected := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	for i, got := range results {
		if got != expected[i] {
			t.Errorf("round %d: expected partition %d, got %d", i, expected[i], got)
		}
	}
}

func TestHashPartitionerSameKey(t *testing.T) {
	// 相同 key 必须总是去同一个分区
	p := NewHashPartitioner()
	numPart := 10
	key := "user_123"

	results := make(map[int]bool)
	for i := 0; i < 100; i++ {
		pid := p.Partition(message.NewRecordWithString(key, "msg"), numPart)
		results[pid] = true
	}

	if len(results) != 1 {
		t.Errorf("same key should always go to same partition, got %d different partitions", len(results))
	}
}

func TestHashPartitionerDifferentKeys(t *testing.T) {
	// 不同 key 应该分布到不同分区
	p := NewHashPartitioner()
	numPart := 10

	partitionSet := make(map[int]bool)
	for i := 0; i < 1000; i++ {
		key := string(rune('a' + i%26)) + string(rune('0'+i%10))
		pid := p.Partition(message.NewRecordWithString(key, "msg"), numPart)
		partitionSet[pid] = true
	}

	// 1000 个不同 key 应该分布到多个分区
	if len(partitionSet) < 3 {
		t.Errorf("expected keys to spread across multiple partitions, got %d", len(partitionSet))
	}
}

func TestHashPartitionerNoKey(t *testing.T) {
	// 没有 key 时退化为轮询
	p := NewHashPartitioner()
	numPart := 3

	pid0 := p.Partition(message.NewRecordWithString("", "a"), numPart)
	pid1 := p.Partition(message.NewRecordWithString("", "b"), numPart)
	pid2 := p.Partition(message.NewRecordWithString("", "c"), numPart)

	// 应该是轮询：0, 1, 2
	if pid0 != 0 || pid1 != 1 || pid2 != 2 {
		t.Errorf("no-key fallback to round-robin: expected [0,1,2], got [%d,%d,%d]",
			pid0, pid1, pid2)
	}
}

func TestManualPartitioner(t *testing.T) {
	p := &ManualPartitioner{PartitionID: 2}
	pid := p.Partition(message.NewRecordWithString("k", "v"), 10)
	if pid != 2 {
		t.Errorf("expected partition 2, got %d", pid)
	}
}

// ─── Producer 测试 ─────────────────────────────────

func TestProducerWithRoundRobin(t *testing.T) {
	topic := NewTopic("test", 3)
	producer := NewProducer(topic, NewRoundRobinPartitioner())

	for i := 0; i < 6; i++ {
		msg, err := producer.Send(message.NewRecordWithString("", "msg"))
		if err != nil {
			t.Fatal(err)
		}
		if msg.Partition != i%3 {
			t.Errorf("expected partition %d, got %d", i%3, msg.Partition)
		}
	}
}

func TestProducerWithHash(t *testing.T) {
	topic := NewTopic("events", 4)
	producer := NewProducer(topic, NewHashPartitioner())

	// 同一个 key 的消息应该去同一个分区
	var targetPart int = -1
	for i := 0; i < 10; i++ {
		msg, err := producer.Send(message.NewRecordWithString("user_1", "event"))
		if err != nil {
			t.Fatal(err)
		}
		if targetPart == -1 {
			targetPart = msg.Partition
		} else if msg.Partition != targetPart {
			t.Errorf("same key should go to same partition: expected %d, got %d",
				targetPart, msg.Partition)
		}
	}
}

func TestProducerSendBatch(t *testing.T) {
	topic := NewTopic("batch", 2)
	producer := NewProducer(topic, NewRoundRobinPartitioner())

	records := []message.Record{
		message.NewRecordWithString("", "a"),
		message.NewRecordWithString("", "b"),
		message.NewRecordWithString("", "c"),
		message.NewRecordWithString("", "d"),
	}
	msgs, err := producer.SendBatch(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 4 {
		t.Errorf("expected 4 messages, got %d", len(msgs))
	}
	// 验证 offset
	for i, msg := range msgs {
		if msg.Offset != int64(i/2) {
			t.Errorf("msg %d: expected offset %d, got %d", i, i/2, msg.Offset)
		}
	}
}

func TestProducerSendToPartition(t *testing.T) {
	topic := NewTopic("manual", 3)
	producer := NewProducer(topic, NewRoundRobinPartitioner())

	msg, err := producer.SendToPartition(message.NewRecordWithString("", "data"), 2)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Partition != 2 {
		t.Errorf("expected partition 2, got %d", msg.Partition)
	}
}
