// Package topic — Kafka 主题
//
// Topic 是 Kafka 中最上层的逻辑概念：
//   - Topic = 多个 Partition 的集合
//   - Producer 发送到 Topic（由分区策略决定去哪个分区）
//   - Consumer 从 Topic 订阅（消费一个或多个分区）
//
// 核心规则：
//   - 同一 Topic 的不同分区之间没有顺序保证
//   - 同一分区内消息严格有序（这是 Kafka 最重要的保证之一）
//   - 分区数 = 期望的最大并行消费者数
package mini_kafka

import (
	"fmt"
	"sync/atomic"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

// Topic Kafka 主题
type Topic struct {
	name       string                  // Topic 名称
	partitions map[int]*Partition      // 分区集合
	rrCounter  atomic.Int64            // 轮询计数器（原子操作，并发安全）
}

// NewTopic 创建新 Topic
func NewTopic(name string, numPartitions int) *Topic {
	partitions := make(map[int]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitions[i] = NewPartition(i)
	}
	return &Topic{
		name:       name,
		partitions: partitions,
	}
}

// Name 返回 Topic 名称
func (t *Topic) Name() string { return t.name }

// NumPartitions 返回分区数量
func (t *Topic) NumPartitions() int { return len(t.partitions) }

// Partition 获取指定分区
func (t *Topic) Partition(id int) (*Partition, error) {
	p, ok := t.partitions[id]
	if !ok {
		return nil, fmt.Errorf("partition %d does not exist, topic '%s' has %d partitions",
			id, t.name, len(t.partitions))
	}
	return p, nil
}

// Partitions 返回所有分区
func (t *Topic) Partitions() []*Partition {
	result := make([]*Partition, 0, len(t.partitions))
	for i := 0; i < len(t.partitions); i++ {
		result = append(result, t.partitions[i])
	}
	return result
}

// Send 发送消息到 Topic
//
// 对应 Kafka 的 Producer.send()。
// 如果指定 partition，直接发送到该分区。
// 否则使用轮询策略。
func (t *Topic) Send(record message.Record, partition ...int) (*message.Message, error) {
	var pid int
	if len(partition) > 0 {
		pid = partition[0]
	} else {
		// 轮询策略：利用原子操作实现并发安全的 round-robin
		pid = int(t.rrCounter.Add(1)-1) % len(t.partitions)
	}

	p, err := t.Partition(pid)
	if err != nil {
		return nil, err
	}
	msg := p.Append(record)
	return msg, nil
}

// SendBatch 批量发送
func (t *Topic) SendBatch(records []message.Record, partition ...int) ([]*message.Message, error) {
	msgs := make([]*message.Message, 0, len(records))
	for _, r := range records {
		msg, err := t.Send(r, partition...)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// Consume 从指定分区消费消息
func (t *Topic) Consume(partitionID int, offset int64, maxCount int) ([]*message.Message, error) {
	p, err := t.Partition(partitionID)
	if err != nil {
		return nil, err
	}
	return p.Read(offset, maxCount), nil
}

// TotalMessages 所有分区的消息总数
func (t *Topic) TotalMessages() int {
	total := 0
	for _, p := range t.partitions {
		total += p.Size()
	}
	return total
}

func (t *Topic) String() string {
	return fmt.Sprintf("Topic(name='%s', partitions=%d, totalMessages=%d)",
		t.name, len(t.partitions), t.TotalMessages())
}
