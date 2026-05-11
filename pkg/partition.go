// Package partition — Kafka 分区
//
// 分区是 Kafka 存储和并行消费的基础：
//   - 每个分区是一个有序的、只追加的消息序列
//   - 消息通过 offset（位移）定位，从 0 开始递增
//   - 分区是并行消费的最小单位
//
// 类比：Partition 就像一个只追加的日志文件。
// 你只能往末尾写（append），不能修改已有内容。
// 每行有一个行号（offset），从 0 开始。
package mini_kafka

import (
	"fmt"
	"sync"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

// Partition Kafka 分区
//
// 核心设计：
//   - 内部维护一个消息切片（真正的 Kafka 用磁盘文件 + mmap）
//   - 每条消息有唯一的 offset
//   - 线程安全：用 sync.RWMutex 保护并发读写
//   - Go 的并发优势：多个 goroutine 可以同时读不同的分区
type Partition struct {
	id       int                // 分区编号
	messages []*message.Message // 消息存储
	nextOff  int64              // 下一条消息的 offset
	mu       sync.RWMutex       // 读写锁（真正的 Kafka 用分段锁）
}

// NewPartition 创建新分区
func NewPartition(id int) *Partition {
	return &Partition{
		id:       id,
		messages: make([]*message.Message, 0),
		nextOff:  0,
	}
}

// ID 返回分区编号
func (p *Partition) ID() int {
	return p.id
}

// Size 返回消息数量
func (p *Partition) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.messages)
}

// LatestOffset 返回最新消息的 offset，-1 表示分区为空
func (p *Partition) LatestOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.messages) == 0 {
		return -1
	}
	return p.messages[len(p.messages)-1].Offset
}

// NextOffset 返回下一条消息将要写入的 offset
func (p *Partition) NextOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOff
}

// Append 追加一条消息到分区末尾
//
// 对应 Kafka 的 Log.append()。
// 这是分区唯一的写入方式——只追加，不修改。
//
// 线程安全：写锁保护。真正的 Kafka 也保证分区内写入串行化。
func (p *Partition) Append(record message.Record) *message.Message {
	p.mu.Lock()
	defer p.mu.Unlock()

	msg := message.NewMessage(p.nextOff, p.id, record)
	p.messages = append(p.messages, &msg)
	p.nextOff++
	return &msg
}

// AppendBatch 批量追加
func (p *Partition) AppendBatch(records []message.Record) []*message.Message {
	msgs := make([]*message.Message, 0, len(records))
	for _, r := range records {
		msgs = append(msgs, p.Append(r))
	}
	return msgs
}

// Read 从指定 offset 开始读取消息
//
// 对应 Kafka 的 Fetch 请求。
// offset: 起始 offset（包含）
// maxCount: 最多读取多少条
func (p *Partition) Read(offset int64, maxCount int) []*message.Message {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset < 0 || offset >= p.nextOff {
		return nil
	}

	start := offset
	// truncate 后 offset 和数组索引可能不一致，需要找到正确的起始位置
	baseOffset := p.messages[0].Offset
	localStart := int(start - baseOffset)
	if localStart < 0 || localStart >= len(p.messages) {
		return nil
	}

	end := localStart + maxCount
	if end > len(p.messages) {
		end = len(p.messages)
	}

	result := make([]*message.Message, end-localStart)
	copy(result, p.messages[localStart:end])
	return result
}

// ReadAll 读取分区所有消息
func (p *Partition) ReadAll() []*message.Message {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*message.Message, len(p.messages))
	copy(result, p.messages)
	return result
}

// Get 读取指定 offset 的单条消息
func (p *Partition) Get(offset int64) *message.Message {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.messages) == 0 {
		return nil
	}
	baseOffset := p.messages[0].Offset
	localIdx := int(offset - baseOffset)
	if localIdx < 0 || localIdx >= len(p.messages) {
		return nil
	}
	return p.messages[localIdx]
}

// Truncate 截断指定 offset 之前的消息（日志清理）
//
// 对应 Kafka 的日志保留策略（log retention）。
// 真正的 Kafka 会删除旧的 Segment 文件。
func (p *Partition) Truncate(beforeOffset int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.messages) == 0 {
		return 0
	}

	baseOffset := p.messages[0].Offset
	cutIdx := int(beforeOffset - baseOffset)
	if cutIdx <= 0 {
		return 0
	}
	if cutIdx > len(p.messages) {
		cutIdx = len(p.messages)
	}

	deleted := cutIdx
	p.messages = p.messages[cutIdx:]
	return deleted
}

func (p *Partition) String() string {
	return fmt.Sprintf("Partition(id=%d, size=%d, latestOffset=%d)",
		p.id, p.Size(), p.LatestOffset())
}
