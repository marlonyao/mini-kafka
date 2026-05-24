// Package partition — Kafka 分区（持久化版）
//
// 分区是 Kafka 存储和并行消费的基础：
//   - 每个分区是一个有序的、只追加的消息序列
//   - 消息通过 offset（位移）定位，从 0 开始递增
//   - 分区是并行消费的最小单位
//
// Step 5 改进：消息持久化到磁盘，使用 Segment 日志分段。
//   - 每个 Partition 有独立的数据目录
//   - 消息追加写入 .log 文件（length-prefix 编码）
//   - .index 文件记录 offset → 文件位置的映射
//   - 当 segment 达到 1MB 时自动滚动创建新 segment
//   - 支持按 retentionMs 清理过期 segment
//   - 启动时自动从磁盘恢复已有数据
//
// 类比：Partition 就像一个按册分卷的账本。
// 每写满一本（1MB），就换一本新的（新 Segment）。
// 旧账本定期清理（retention），但当前正在写的这本永远保留。
package mini_kafka

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

const (
	// defaultSegmentSize segment 默认容量：1MB
	defaultSegmentSize = 1 * 1024 * 1024
	// defaultRetentionMs 默认日志保留时间：7 天（毫秒）
	defaultRetentionMs = 7 * 24 * 60 * 60 * 1000
)

// Partition Kafka 分区
//
// 核心设计：
//   - 消息存储在磁盘 Segment 文件中（不再是纯内存）
//   - 使用追加写（append-only），顺序写磁盘，性能接近内存
//   - 线程安全：用 sync.RWMutex 保护并发读写
//   - Go 的并发优势：多个 goroutine 可以同时读不同的分区
//
// Step 5 重构后，Partition 的 API 完全保持向后兼容。
type Partition struct {
	id          int        // 分区编号
	dir         string     // 数据目录
	segments    []*Segment // 所有 segment（按 baseOffset 排序）
	active      *Segment   // 当前活跃 segment（可追加写入）
	nextOff     int64      // 下一条消息的 offset
	startOffset int64      // 逻辑起始 offset（被 Truncate 或 Cleanup 移除的）
	maxSegSize  int64      // segment 容量上限（字节）
	retentionMs int64      // 日志保留时间（毫秒）
	mu          sync.RWMutex
}

// NewPartition 创建新分区（自动分配临时数据目录）
//
// 为保持向后兼容，不修改函数签名。
// 内部使用系统临时目录（/tmp/mini-kafka-p{id}-{timestamp}/）。
// 测试或生产环境如需显式控制目录，请使用 NewPartitionWithDir。
func NewPartition(id int) *Partition {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("mini-kafka-p%d-%d", id, time.Now().UnixNano()))
	p, err := NewPartitionWithDir(id, dir)
	if err != nil {
		// 临时目录创建失败属于极罕见情况，直接 panic（和内存分配失败类似）
		panic(fmt.Sprintf("create partition %d failed: %v", id, err))
	}
	return p
}

// NewPartitionWithDir 指定数据目录创建分区
//
// 生产环境应使用此方法，显式指定持久化目录。
// 启动时会自动加载 dir 中已有的 segment 文件并恢复状态。
func NewPartitionWithDir(id int, dir string) (*Partition, error) {
	p := &Partition{
		id:          id,
		dir:         dir,
		segments:    make([]*Segment, 0),
		nextOff:     0,
		startOffset: 0,
		maxSegSize:  defaultSegmentSize,
		retentionMs: defaultRetentionMs,
	}
	if err := p.loadSegments(); err != nil {
		return nil, err
	}
	return p, nil
}

// loadSegments 启动时从磁盘加载已有 segment
//
// 流程：
//   1. 确保数据目录存在
//   2. 扫描目录中的 .log 文件，解析 baseOffset
//   3. 按 baseOffset 排序，依次 OpenSegment
//   4. 计算 nextOff = 最后一个 segment 的 baseOffset + 消息数
//   5. 如果没有 segment，创建 baseOffset=0 的初始 segment
func (p *Partition) loadSegments() error {
	if err := os.MkdirAll(p.dir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", p.dir, err)
	}

	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", p.dir, err)
	}

	var baseOffsets []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".log") {
			base, err := parseBaseOffset(entry.Name())
			if err != nil {
				continue // 跳过非法文件名
			}
			baseOffsets = append(baseOffsets, base)
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, base := range baseOffsets {
		seg, err := OpenSegment(p.dir, base, p.maxSegSize)
		if err != nil {
			return fmt.Errorf("open segment base=%d: %w", base, err)
		}
		p.segments = append(p.segments, seg)
	}

	if len(p.segments) > 0 {
		p.active = p.segments[len(p.segments)-1]
		p.nextOff = p.active.BaseOffset() + int64(p.active.NumMessages())
		p.startOffset = p.segments[0].BaseOffset()
	} else {
		seg, err := NewSegment(p.dir, 0, p.maxSegSize)
		if err != nil {
			return fmt.Errorf("create initial segment: %w", err)
		}
		p.segments = append(p.segments, seg)
		p.active = seg
		p.nextOff = 0
		p.startOffset = 0
	}

	return nil
}

// ID 返回分区编号
func (p *Partition) ID() int {
	return p.id
}

// Size 返回消息数量（逻辑数量，扣除被 Truncate 的）
func (p *Partition) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int(p.nextOff - p.startOffset)
}

// LatestOffset 返回最新消息的 offset，-1 表示分区为空
func (p *Partition) LatestOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.nextOff <= p.startOffset {
		return -1
	}
	return p.nextOff - 1
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
// 如果当前 segment 已满，自动滚动创建新 segment。
//
// 线程安全：写锁保护。真正的 Kafka 也保证分区内写入串行化。
func (p *Partition) Append(record message.Record) *message.Message {
	p.mu.Lock()
	defer p.mu.Unlock()

	msg := message.NewMessage(p.nextOff, p.id, record)

	// segment 滚动检查
	if p.active.Size() >= p.maxSegSize {
		// 注意：不关闭旧 segment 的文件句柄，后续仍需读取
		newSeg, err := NewSegment(p.dir, p.nextOff, p.maxSegSize)
		if err != nil {
			panic(fmt.Sprintf("roll segment failed: %v", err))
		}
		p.segments = append(p.segments, newSeg)
		p.active = newSeg
	}

	if err := p.active.Append(&msg); err != nil {
		panic(fmt.Sprintf("append message failed: %v", err))
	}

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

	if offset < p.startOffset {
		offset = p.startOffset
	}
	if offset < 0 || offset >= p.nextOff || maxCount <= 0 {
		return nil
	}

	return p.readUnlocked(offset, maxCount)
}

// readUnlocked 无锁读取（调用方必须已持有读锁或写锁）
func (p *Partition) readUnlocked(offset int64, maxCount int) []*message.Message {
	result := make([]*message.Message, 0, maxCount)
	remaining := maxCount
	currentOffset := offset

	for remaining > 0 && currentOffset < p.nextOff {
		seg := p.findSegment(currentOffset)
		if seg == nil {
			break
		}
		msgs, err := seg.ReadRange(currentOffset, remaining)
		if err != nil {
			break
		}
		if len(msgs) == 0 {
			break
		}
		result = append(result, msgs...)
		remaining -= len(msgs)
		currentOffset += int64(len(msgs))
	}

	return result
}

// ReadAll 读取分区所有消息
func (p *Partition) ReadAll() []*message.Message {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.nextOff <= p.startOffset {
		return nil
	}
	return p.readUnlocked(p.startOffset, int(p.nextOff-p.startOffset))
}

// Get 读取指定 offset 的单条消息
func (p *Partition) Get(offset int64) *message.Message {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset < p.startOffset || offset >= p.nextOff {
		return nil
	}

	seg := p.findSegment(offset)
	if seg == nil {
		return nil
	}
	msg, err := seg.Read(offset)
	if err != nil {
		return nil
	}
	return msg
}

// findSegment 查找包含指定 offset 的 segment（调用方必须已持有锁）
func (p *Partition) findSegment(offset int64) *Segment {
	for _, seg := range p.segments {
		if seg.Contains(offset) {
			return seg
		}
	}
	return nil
}

// Truncate 截断指定 offset 之前的消息（逻辑截断）
//
// 对应 Kafka 的日志保留策略（log retention）。
// 设置逻辑起始 offset，不实际删除文件（由 Cleanup 负责物理清理）。
// 返回被截断的消息数量。
func (p *Partition) Truncate(beforeOffset int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if beforeOffset <= p.startOffset {
		return 0
	}
	if beforeOffset > p.nextOff {
		beforeOffset = p.nextOff
	}

	deleted := int(beforeOffset - p.startOffset)
	p.startOffset = beforeOffset
	return deleted
}

// Cleanup 清理过期的 segment（日志保留）
//
// 按 retentionMs 检查每个非活跃 segment。
// 只删除完整的 segment，不删除当前活跃 segment。
// 如果 segment 的最后一条消息时间已超过 retentionMs，则整个 segment 被删除。
// 返回删除的消息数量。
func (p *Partition) Cleanup() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.retentionMs <= 0 {
		return 0
	}

	cutoff := time.Now().Add(-time.Duration(p.retentionMs) * time.Millisecond)
	deleted := 0
	var kept []*Segment

	for _, seg := range p.segments {
		if seg == p.active {
			kept = append(kept, seg)
			continue
		}
		lastTime, err := seg.LastMessageTime()
		if err != nil {
			kept = append(kept, seg)
			continue
		}
		if lastTime.Before(cutoff) {
			seg.Close()
			os.Remove(seg.LogPath())
			os.Remove(seg.IndexPath())
			deleted += seg.NumMessages()
		} else {
			kept = append(kept, seg)
		}
	}

	p.segments = kept
	if len(p.segments) > 0 {
		p.startOffset = p.segments[0].BaseOffset()
	}
	return deleted
}

// SetRetentionMs 设置日志保留时间（毫秒，主要用于测试）
func (p *Partition) SetRetentionMs(ms int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retentionMs = ms
}

// SetMaxSegmentSize 设置 segment 最大容量（字节，主要用于测试）
func (p *Partition) SetMaxSegmentSize(size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxSegSize = size
}

// CleanupData 删除整个数据目录（测试清理用）
func (p *Partition) CleanupData() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, seg := range p.segments {
		seg.Close()
	}
	p.segments = nil
	p.active = nil

	return os.RemoveAll(p.dir)
}

// Close 关闭所有 segment 文件
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, seg := range p.segments {
		seg.Close()
	}
	return nil
}

// DataDir 返回数据目录路径
func (p *Partition) DataDir() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dir
}

func (p *Partition) String() string {
	return fmt.Sprintf("Partition(id=%d, size=%d, latestOffset=%d)",
		p.id, p.Size(), p.LatestOffset())
}
