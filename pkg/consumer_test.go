package mini_kafka

import (
	"testing"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

// ─── ConsumerOffset 测试 ──────────────────────────

func TestConsumerOffset(t *testing.T) {
	co := NewConsumerOffset()

	// 初始 offset 为 0
	if co.Get("group1", "topic1", 0) != 0 {
		t.Error("initial offset should be 0")
	}

	co.Commit("group1", "topic1", 0, 5)
	if co.Get("group1", "topic1", 0) != 5 {
		t.Errorf("expected offset=5, got %d", co.Get("group1", "topic1", 0))
	}

	// 不同 group 独立
	if co.Get("group2", "topic1", 0) != 0 {
		t.Error("different group should have separate offsets")
	}
}

// ─── Consumer 测试 ─────────────────────────────────

func TestConsumerPoll(t *testing.T) {
	topic := NewTopic("test", 2)
	// 往两个分区各放 3 条消息
	for i := 0; i < 3; i++ {
		topic.Send(message.NewRecordWithString("k", "msg"), 0)
		topic.Send(message.NewRecordWithString("k", "msg"), 1)
	}

	co := NewConsumerOffset()
	consumer := NewConsumer("c1", "g1", topic, co)
	consumer.Assign([]int{0, 1})

	msgs := consumer.Poll(100)
	if len(msgs) != 6 {
		t.Errorf("expected 6 messages, got %d", len(msgs))
	}
}

func TestConsumerPollIncremental(t *testing.T) {
	// 测试增量消费：poll 只返回新的消息
	topic := NewTopic("test", 1)
	co := NewConsumerOffset()
	consumer := NewConsumer("c1", "g1", topic, co)
	consumer.Assign([]int{0})

	// 第一批
	for i := 0; i < 3; i++ {
		topic.Send(message.NewRecordWithString("", "msg"), 0)
	}
	msgs1 := consumer.Poll(100)
	if len(msgs1) != 3 {
		t.Errorf("batch1: expected 3, got %d", len(msgs1))
	}

	// 再写 2 条
	for i := 0; i < 2; i++ {
		topic.Send(message.NewRecordWithString("", "msg"), 0)
	}
	msgs2 := consumer.Poll(100)
	if len(msgs2) != 2 {
		t.Errorf("batch2: expected 2, got %d", len(msgs2))
	}

	// 没有新消息
	msgs3 := consumer.Poll(100)
	if len(msgs3) != 0 {
		t.Errorf("batch3: expected 0, got %d", len(msgs3))
	}
}

func TestConsumerCommitAndResume(t *testing.T) {
	// 测试 offset 提交和恢复
	topic := NewTopic("test", 1)
	for i := 0; i < 10; i++ {
		topic.Send(message.NewRecordWithString("", "msg"), 0)
	}

	co := NewConsumerOffset()

	// 第一个消费者：消费前5条并提交
	c1 := NewConsumer("c1", "g1", topic, co)
	c1.Assign([]int{0})
	msgs := c1.Poll(5)
	if len(msgs) != 5 {
		t.Fatalf("expected 5, got %d", len(msgs))
	}
	c1.CommitOffsets()

	// 验证 offset 已提交
	committed := co.Get("g1", "test", 0)
	if committed != 5 {
		t.Errorf("expected committed offset=5, got %d", committed)
	}

	// 新消费者从已提交的 offset 恢复
	c2 := NewConsumer("c2", "g1", topic, co)
	c2.Assign([]int{0})
	msgs2 := c2.Poll(100)
	if len(msgs2) != 5 {
		t.Errorf("expected 5 remaining messages, got %d", len(msgs2))
	}
}

func TestConsumerSeek(t *testing.T) {
	topic := NewTopic("test", 1)
	for i := 0; i < 10; i++ {
		topic.Send(message.NewRecordWithString("", "msg"), 0)
	}

	co := NewConsumerOffset()
	c := NewConsumer("c1", "g1", topic, co)
	c.Assign([]int{0})

	// seek 到 offset=7
	c.Seek(0, 7)
	msgs := c.Poll(100)
	if len(msgs) != 3 {
		t.Errorf("expected 3 messages from offset 7, got %d", len(msgs))
	}
}

func TestConsumerSeekToBeginning(t *testing.T) {
	topic := NewTopic("test", 1)
	for i := 0; i < 5; i++ {
		topic.Send(message.NewRecordWithString("", "msg"), 0)
	}

	co := NewConsumerOffset()
	c := NewConsumer("c1", "g1", topic, co)
	c.Assign([]int{0})

	// 先消费一些
	c.Poll(3)
	// seek 回开头
	c.SeekToBeginning(0)
	msgs := c.Poll(100)
	if len(msgs) != 5 {
		t.Errorf("expected 5 messages, got %d", len(msgs))
	}
}

// ─── ConsumerGroup 测试 ────────────────────────────

func TestConsumerGroupBasic(t *testing.T) {
	topic := NewTopic("orders", 3)
	for i := 0; i < 9; i++ {
		topic.Send(message.NewRecordWithString("", "order"))
	}

	co := NewConsumerOffset()
	cg := NewConsumerGroup("analytics", topic, co)

	c1 := NewConsumer("c1", "analytics", topic, co)
	c2 := NewConsumer("c2", "analytics", topic, co)
	c3 := NewConsumer("c3", "analytics", topic, co)

	cg.Join(c1)
	cg.Join(c2)
	cg.Join(c3)

	// 3 个消费者，3 个分区 → 每人 1 个分区
	for _, c := range []*Consumer{c1, c2, c3} {
		parts := c.AssignedPartitions()
		if len(parts) != 1 {
			t.Errorf("consumer %s: expected 1 partition, got %d", c.id, len(parts))
		}
	}

	// 并行消费
	results := cg.PollAll(100)
	totalMsgs := 0
	for _, msgs := range results {
		totalMsgs += len(msgs)
	}
	if totalMsgs != 9 {
		t.Errorf("expected 9 total messages, got %d", totalMsgs)
	}
}

func TestConsumerGroupMoreConsumersThanPartitions(t *testing.T) {
	// 消费者比分区多 → 多余的空闲
	topic := NewTopic("test", 2)
	co := NewConsumerOffset()
	cg := NewConsumerGroup("g1", topic, co)

	c1 := NewConsumer("c1", "g1", topic, co)
	c2 := NewConsumer("c2", "g1", topic, co)
	c3 := NewConsumer("c3", "g1", topic, co) // 这个会空闲
	c4 := NewConsumer("c4", "g1", topic, co) // 这个也会空闲

	cg.Join(c1)
	cg.Join(c2)
	cg.Join(c3)
	cg.Join(c4)

	// c3, c4 没有分配到分区
	hasEmpty := false
	for _, c := range []*Consumer{c3, c4} {
		if len(c.AssignedPartitions()) == 0 {
			hasEmpty = true
		}
	}
	if !hasEmpty {
		t.Error("expected some consumers to have no partitions")
	}
}

func TestConsumerGroupRebalance(t *testing.T) {
	// 消费者离开后触发再均衡
	topic := NewTopic("test", 4)
	co := NewConsumerOffset()
	cg := NewConsumerGroup("g1", topic, co)

	c1 := NewConsumer("c1", "g1", topic, co)
	c2 := NewConsumer("c2", "g1", topic, co)
	cg.Join(c1)
	cg.Join(c2)

	// 2 个消费者，4 个分区 → 每人 2 个
	total1 := len(c1.AssignedPartitions()) + len(c2.AssignedPartitions())
	if total1 != 4 {
		t.Errorf("expected 4 assigned, got %d", total1)
	}

	// c2 离开 → c1 消费所有 4 个分区
	cg.Leave("c2")
	if len(c1.AssignedPartitions()) != 4 {
		t.Errorf("after c2 left, c1 should have 4 partitions, got %d",
			len(c1.AssignedPartitions()))
	}
}

func TestConsumerGroupNoDuplicateConsumption(t *testing.T) {
	// 同一组内，每个分区只被一个消费者消费
	topic := NewTopic("test", 3)
	for i := 0; i < 9; i++ {
		topic.Send(message.NewRecordWithString("", "msg"), i%3) // 均匀到3个分区
	}

	co := NewConsumerOffset()
	cg := NewConsumerGroup("g1", topic, co)

	c1 := NewConsumer("c1", "g1", topic, co)
	c2 := NewConsumer("c2", "g1", topic, co)
	cg.Join(c1)
	cg.Join(c2)

	results := cg.PollAll(100)

	// 检查每个分区只被一个消费者消费
	consumedPartitions := make(map[int]string) // partition → consumerID
	for cid, msgs := range results {
		for _, msg := range msgs {
			if prev, exists := consumedPartitions[msg.Partition]; exists && prev != cid {
				t.Errorf("partition %d consumed by both %s and %s",
					msg.Partition, prev, cid)
			}
			consumedPartitions[msg.Partition] = cid
		}
	}

	// 消费了所有消息
	totalMsgs := 0
	for _, msgs := range results {
		totalMsgs += len(msgs)
	}
	if totalMsgs != 9 {
		t.Errorf("expected 9 total messages, got %d", totalMsgs)
	}
}
