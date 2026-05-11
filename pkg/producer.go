// Package producer — Kafka 生产者
//
// Producer 是 Kafka 的写入端，负责把消息发送到正确的分区。
//
// 核心问题：一条消息应该去哪个分区？
// Kafka 提供了三种分区策略：
//
//  1. 指定分区 — 直接发到指定分区（最高优先级）
//  2. 按键哈希 — 相同 key 的消息总是去同一个分区（保证顺序）
//  3. 轮询 — 均匀分布到各分区（默认策略）
//
// 为什么按键哈希能保证顺序？
//   因为同一个 key 的消息总是在同一个分区，而分区内部是有序的。
//   比如同一个用户（key=user_123）的所有操作总是按顺序排列。
//
// Go 实现：利用 hash/fnv 做一致性哈希，比 Python 版更贴近生产实现。
package mini_kafka

import (
	"hash/fnv"
	"log"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

// Partitioner 分区策略接口
//
// 对应 Kafka 的 Partitioner 接口。
// 你可以实现自己的分区策略（比如按地域、按用户类型等）。
type Partitioner interface {
	// Partition 选择目标分区
	//   record: 要发送的记录
	//   numPartitions: 可用分区数
	Partition(record message.Record, numPartitions int) int
}

// RoundRobinPartitioner 轮询分区策略
//
// 对应 Kafka 的 RoundRobinPartitioner。
// 消息均匀分布到各分区，适合没有 key 的场景。
//
// 示例（3个分区）：
//
//	msg0 → P0, msg1 → P1, msg2 → P2, msg3 → P0, msg4 → P1, ...
type RoundRobinPartitioner struct {
	counter int
}

// NewRoundRobinPartitioner 创建轮询分区器
func NewRoundRobinPartitioner() *RoundRobinPartitioner {
	return &RoundRobinPartitioner{counter: 0}
}

func (r *RoundRobinPartitioner) Partition(_ message.Record, numPartitions int) int {
	part := r.counter % numPartitions
	r.counter++
	return part
}

// HashPartitioner 按键哈希分区策略
//
// 对应 Kafka 的默认分区器。
// 相同 key 的消息总是路由到同一个分区。
// 没有 key 时退化为轮询策略。
//
// 为什么这很重要？
//   举例：用户 user_123 的所有订单事件必须按顺序处理。
//   用 user_123 作为 key → 所有事件到同一个分区 → 分区内有序 → 顺序保证 ✅
type HashPartitioner struct {
	rr *RoundRobinPartitioner // 无 key 时的兜底策略
}

// NewHashPartitioner 创建哈希分区器
func NewHashPartitioner() *HashPartitioner {
	return &HashPartitioner{rr: NewRoundRobinPartitioner()}
}

func (h *HashPartitioner) Partition(record message.Record, numPartitions int) int {
	// 没有 key → 退化为轮询
	if record.Key == "" {
		return h.rr.Partition(record, numPartitions)
	}
	// FNV-1a 哈希 → 取模得到分区号
	hasher := fnv.New32a()
	hasher.Write([]byte(record.Key))
	hashVal := hasher.Sum32()
	return int(hashVal % uint32(numPartitions))
}

// ManualPartitioner 指定分区策略
//
// 直接发到用户指定的分区，不做任何计算。
type ManualPartitioner struct {
	PartitionID int
}

func (m *ManualPartitioner) Partition(_ message.Record, _ int) int {
	return m.PartitionID
}

// Producer Kafka 生产者
//
// 对应 Kafka 的 KafkaProducer。
// 负责选择分区策略并将消息发送到 Topic。
//
// 使用方式：
//
//	producer := NewProducer(topic, NewHashPartitioner())
//	msg, err := producer.Send(message.NewRecordWithString("user1", "login"))
type Producer struct {
	topic      *Topic
	partitioner Partitioner
}

// NewProducer 创建生产者
func NewProducer(topic *Topic, partitioner Partitioner) *Producer {
	return &Producer{
		topic:      topic,
		partitioner: partitioner,
	}
}

// Send 发送一条消息
//
// 流程：
//  1. Partitioner 选择目标分区
//  2. 调用 Topic.Send 发送到对应分区
//  3. 返回包含 offset 和 partition 信息的 Message
func (p *Producer) Send(record message.Record) (*message.Message, error) {
	pid := p.partitioner.Partition(record, p.topic.NumPartitions())
	msg, err := p.topic.Send(record, pid)
	if err != nil {
		log.Printf("Failed to send message to partition %d: %v", pid, err)
		return nil, err
	}
	return msg, nil
}

// SendToPartition 发送到指定分区（忽略分区策略）
func (p *Producer) SendToPartition(record message.Record, partition int) (*message.Message, error) {
	return p.topic.Send(record, partition)
}

// SendBatch 批量发送
func (p *Producer) SendBatch(records []message.Record) ([]*message.Message, error) {
	msgs := make([]*message.Message, 0, len(records))
	for _, r := range records {
		msg, err := p.Send(r)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}
