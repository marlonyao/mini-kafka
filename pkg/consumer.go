// Package consumer — Kafka 消费者
//
// Consumer 是 Kafka 的读取端。
//
// Kafka 的消费模型是"拉取"（pull）模式：
//   - Consumer 主动向 Broker 拉取消息（而不是 Broker 推送）
//   - 通过 offset 记录"我读到哪里了"
//   - 可以从任意 offset 开始读（非常灵活）
//
// Consumer Group（消费者组）：
//   - 同一个 Group 内的 Consumer 共同分担 Topic 的所有分区
//   - 每个分区只被组内的一个 Consumer 消费（不重复）
//   - 如果 Consumer 比 分区多，多余的 Consumer 空闲
//   - 如果 Consumer 比 分区少，某些 Consumer 消费多个分区
//
// 这是 Kafka 最强大的消费模型！
//
//   Topic: orders (4个分区)
//   Consumer Group "analytics":
//     Consumer A ← P0, P1
//     Consumer B ← P2, P3
//
//   再加一个 Consumer C → 触发 Rebalance:
//     Consumer A ← P0
//     Consumer B ← P1, P2
//     Consumer C ← P3
package mini_kafka

import (
	"fmt"
	"log"
	"sync"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

// OffsetCommitMode offset 提交模式
type OffsetCommitMode int

const (
	// CommitAuto 自动提交（至少一次语义）
	CommitAuto OffsetCommitMode = iota
	// CommitManual 手动提交（精确一次语义）
	CommitManual
)

// ConsumerOffset 消费位移存储
//
// 对应 Kafka 的 __consumer_offsets 内部 Topic。
// 记录每个 (group, topic, partition) 的消费进度。
type ConsumerOffset struct {
	mu      sync.RWMutex
	offsets map[string]map[int]int64 // key="group:topic" → partition → offset
}

// NewConsumerOffset 创建 offset 存储
func NewConsumerOffset() *ConsumerOffset {
	return &ConsumerOffset{
		offsets: make(map[string]map[int]int64),
	}
}

func (co *ConsumerOffset) key(group, topic string) string {
	return group + ":" + topic
}

// Commit 提交 offset
func (co *ConsumerOffset) Commit(group, topic string, partition int, offset int64) {
	co.mu.Lock()
	defer co.mu.Unlock()
	k := co.key(group, topic)
	if co.offsets[k] == nil {
		co.offsets[k] = make(map[int]int64)
	}
	co.offsets[k][partition] = offset
}

// Get 获取已提交的 offset
func (co *ConsumerOffset) Get(group, topic string, partition int) int64 {
	co.mu.RLock()
	defer co.mu.RUnlock()
	k := co.key(group, topic)
	if co.offsets[k] == nil {
		return 0 // 从头开始
	}
	return co.offsets[k][partition]
}

// Consumer Kafka 消费者
//
// 对应 Kafka 的 KafkaConsumer。
// 每个消费者属于一个 Consumer Group。
type Consumer struct {
	id       string        // 消费者 ID
	group    string        // 消费者组 ID
	topic    *Topic        // 订阅的 Topic
	offsets  *ConsumerOffset // offset 存储
	assigned map[int]int64 // 分配到的分区 → 当前 offset
	mu       sync.Mutex
}

// NewConsumer 创建消费者
func NewConsumer(id, group string, topic *Topic, offsets *ConsumerOffset) *Consumer {
	return &Consumer{
		id:       id,
		group:    group,
		topic:    topic,
		offsets:  offsets,
		assigned: make(map[int]int64),
	}
}

// ID 返回消费者 ID
func (c *Consumer) ID() string { return c.id }

// Group 返回消费者组
func (c *Consumer) Group() string { return c.group }

// Assign 分配分区给消费者
//
// 对应 Kafka 的分区分配结果。
// 分配后，消费者从已提交的 offset 开始消费。
func (c *Consumer) Assign(partitionIDs []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.assigned = make(map[int]int64)
	for _, pid := range partitionIDs {
		// 从已提交的 offset 恢复
		committed := c.offsets.Get(c.group, c.topic.Name(), pid)
		c.assigned[pid] = committed
	}
	log.Printf("[Consumer %s] assigned partitions %v, starting offsets: %v",
		c.id, partitionIDs, c.assigned)
}

// AssignedPartitions 返回已分配的分区列表
func (c *Consumer) AssignedPartitions() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]int, 0, len(c.assigned))
	for pid := range c.assigned {
		result = append(result, pid)
	}
	return result
}

// Poll 拉取消息
//
// 对应 Kafka 的 consumer.poll()。
// 从每个已分配的分区拉取 maxPerPartition 条消息。
//
// 这是 Kafka 的"拉取模型"核心：Consumer 主动拉，不是 Broker 推。
func (c *Consumer) Poll(maxPerPartition int) []*message.Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	var results []*message.Message
	for pid, offset := range c.assigned {
		part, err := c.topic.Partition(pid)
		if err != nil {
			continue
		}
		msgs := part.Read(offset, maxPerPartition)
		if len(msgs) > 0 {
			results = append(results, msgs...)
			// 更新本地 offset
			c.assigned[pid] = msgs[len(msgs)-1].Offset + 1
		}
	}
	return results
}

// PollFromPartition 从指定分区拉取消息
func (c *Consumer) PollFromPartition(partitionID int, maxCount int) []*message.Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	offset, ok := c.assigned[partitionID]
	if !ok {
		return nil
	}

	part, err := c.topic.Partition(partitionID)
	if err != nil {
		return nil
	}

	msgs := part.Read(offset, maxCount)
	if len(msgs) > 0 {
		c.assigned[partitionID] = msgs[len(msgs)-1].Offset + 1
	}
	return msgs
}

// CommitOffsets 提交当前 offset
//
// 对应 Kafka 的 consumer.commitSync()。
// 将本地 offset 持久化到 offset 存储。
func (c *Consumer) CommitOffsets() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for pid, offset := range c.assigned {
		c.offsets.Commit(c.group, c.topic.Name(), pid, offset)
	}
}

// Seek 重置 offset 到指定位置
//
// 对应 Kafka 的 consumer.seek()。
// 可以跳到任意位置开始消费！
func (c *Consumer) Seek(partitionID int, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.assigned[partitionID]; !ok {
		return fmt.Errorf("partition %d not assigned to consumer %s", partitionID, c.id)
	}
	c.assigned[partitionID] = offset
	return nil
}

// SeekToBeginning 从分区开头开始消费
func (c *Consumer) SeekToBeginning(partitionID int) {
	c.Seek(partitionID, 0)
}

// ConsumerGroup 消费者组
//
// 对应 Kafka 的 Consumer Group。
// 管理组内消费者的分区分配。
type ConsumerGroup struct {
	groupID   string
	topic     *Topic
	consumers []*Consumer
	offsets   *ConsumerOffset
	mu        sync.Mutex
}

// NewConsumerGroup 创建消费者组
func NewConsumerGroup(groupID string, topic *Topic, offsets *ConsumerOffset) *ConsumerGroup {
	return &ConsumerGroup{
		groupID: groupID,
		topic:   topic,
		offsets: offsets,
	}
}

// Join 消费者加入组
//
// 对应 Kafka 的 JoinGroup 请求。
// 加入后会触发 Rebalance（Step 4 会深入讲解）。
func (cg *ConsumerGroup) Join(consumer *Consumer) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.consumers = append(cg.consumers, consumer)
	cg.rebalance()
}

// Leave 消费者离开组
func (cg *ConsumerGroup) Leave(consumerID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	filtered := make([]*Consumer, 0, len(cg.consumers)-1)
	for _, c := range cg.consumers {
		if c.id != consumerID {
			filtered = append(filtered, c)
		}
	}
	cg.consumers = filtered
	cg.rebalance()
}

// Consumers 返回组内所有消费者
func (cg *ConsumerGroup) Consumers() []*Consumer {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	result := make([]*Consumer, len(cg.consumers))
	copy(result, cg.consumers)
	return result
}

// rebalance 重新分配分区
//
// 核心逻辑：把 Topic 的所有分区均匀分配给组内的消费者。
// 策略：Range Assignor（Kafka 默认）
//   - 分区按编号排序
//   - 前 n 个分区给第一个消费者，接下来 n 个给第二个，依此类推
func (cg *ConsumerGroup) rebalance() {
	numPartitions := cg.topic.NumPartitions()
	numConsumers := len(cg.consumers)

	if numConsumers == 0 {
		return
	}

	// 均匀分配（Range Assignor）
	assignments := make(map[string][]int) // consumerID → partitionIDs
	for pid := 0; pid < numPartitions; pid++ {
		cid := pid % numConsumers
		consumer := cg.consumers[cid]
		assignments[consumer.id] = append(assignments[consumer.id], pid)
	}

	for _, c := range cg.consumers {
		if parts, ok := assignments[c.id]; ok {
			c.Assign(parts)
		} else {
			c.Assign(nil)
		}
	}

	log.Printf("[ConsumerGroup %s] rebalanced: %d partitions → %d consumers",
		cg.groupID, numPartitions, numConsumers)
}

// PollAll 组内所有消费者同时拉取（模拟并行消费）
func (cg *ConsumerGroup) PollAll(maxPerPartition int) map[string][]*message.Message {
	cg.mu.Lock()
	consumers := make([]*Consumer, len(cg.consumers))
	copy(consumers, cg.consumers)
	cg.mu.Unlock()

	results := make(map[string][]*message.Message)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, c := range consumers {
		wg.Add(1)
		go func(consumer *Consumer) {
			defer wg.Done()
			msgs := consumer.Poll(maxPerPartition)
			mu.Lock()
			results[consumer.id] = msgs
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	return results
}

// CommitAll 提交所有消费者的 offset
func (cg *ConsumerGroup) CommitAll() {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	for _, c := range cg.consumers {
		c.CommitOffsets()
	}
}
