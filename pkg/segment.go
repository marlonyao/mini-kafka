// Package segment — Kafka 日志分段
//
// Segment 是 Kafka 持久化的核心单元：
//   - 每个 Segment 是一对文件：.log（消息数据）和 .index（偏移索引）
//   - .log 使用追加写（append-only），顺序写磁盘
//   - .index 记录 offset → 文件位置的映射，加速随机读取
//   - 当 Segment 达到设定大小（默认 1MB）时，滚动创建新 Segment
//
// 文件命名格式：{起始offset，补零20位}.log / .index
// 示例：00000000000000000000.log
//
// 类比：Segment 就像日志文件按大小切分后的一个"卷"。
// 每写满一本（1MB），就换一本新的，旧的本子封存起来。
package mini_kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

const (
	// indexEntrySize 每个索引条目的大小（字节）
	// 格式：offset(int64, 8字节) + position(int64, 8字节)
	indexEntrySize = 16
)

// Segment 日志分段
//
// 对应 Kafka 的 LogSegment。
// 管理一对磁盘文件，提供追加写入和按 offset 读取的能力。
type Segment struct {
	baseOffset int64    // 该 segment 起始的 offset
	logPath    string   // .log 文件路径
	indexPath  string   // .index 文件路径
	logFile    *os.File // .log 文件句柄
	indexFile  *os.File // .index 文件句柄
	position   int64    // 当前 log 写入位置（即文件末尾偏移）
	size       int64    // 当前 segment 已写字节数（近似）
	numMsgs    int      // 当前 segment 消息数
	maxSize    int64    // 最大容量，超过则触发滚动
	mu         sync.Mutex
}

// NewSegment 创建新的 Segment（空）
//
// dir: 数据目录
// baseOffset: 该 segment 的起始 offset（用于文件命名）
// maxSize: segment 容量上限（字节）
func NewSegment(dir string, baseOffset int64, maxSize int64) (*Segment, error) {
	logName := fmt.Sprintf("%020d.log", baseOffset)
	indexName := fmt.Sprintf("%020d.index", baseOffset)
	logPath := filepath.Join(dir, logName)
	indexPath := filepath.Join(dir, indexName)

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("create log file %s: %w", logPath, err)
	}
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("create index file %s: %w", indexPath, err)
	}

	return &Segment{
		baseOffset: baseOffset,
		logPath:    logPath,
		indexPath:  indexPath,
		logFile:    logFile,
		indexFile:  indexFile,
		position:   0,
		size:       0,
		numMsgs:    0,
		maxSize:    maxSize,
	}, nil
}

// OpenSegment 打开已存在的 Segment（恢复场景）
//
// 启动时从磁盘扫描到已有 .log / .index 文件后，调用此方法恢复。
// 会从文件状态推断 position、size、numMsgs。
func OpenSegment(dir string, baseOffset int64, maxSize int64) (*Segment, error) {
	logName := fmt.Sprintf("%020d.log", baseOffset)
	indexName := fmt.Sprintf("%020d.index", baseOffset)
	logPath := filepath.Join(dir, logName)
	indexPath := filepath.Join(dir, indexName)

	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", logPath, err)
	}
	indexFile, err := os.OpenFile(indexPath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open index file %s: %w", indexPath, err)
	}

	logStat, _ := logFile.Stat()
	indexStat, _ := indexFile.Stat()

	numMsgs := int(indexStat.Size() / indexEntrySize)

	return &Segment{
		baseOffset: baseOffset,
		logPath:    logPath,
		indexPath:  indexPath,
		logFile:    logFile,
		indexFile:  indexFile,
		position:   logStat.Size(),
		size:       logStat.Size(),
		numMsgs:    numMsgs,
		maxSize:    maxSize,
	}, nil
}

// BaseOffset 返回起始 offset
func (s *Segment) BaseOffset() int64 {
	return s.baseOffset
}

// Size 返回当前字节数
func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// NumMessages 返回消息数
func (s *Segment) NumMessages() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numMsgs
}

// IsFull 是否达到最大容量
func (s *Segment) IsFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size >= s.maxSize
}

// Contains 判断 offset 是否在该 segment 范围内
func (s *Segment) Contains(offset int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return offset >= s.baseOffset && offset < s.baseOffset+int64(s.numMsgs)
}

// Append 追加一条消息
//
// 编码格式：4字节长度（大端序）+ JSON 编码的消息数据
// 同时更新 .index 文件，记录 offset → 文件位置映射。
func (s *Segment) Append(msg *message.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message offset=%d: %w", msg.Offset, err)
	}
	length := uint32(len(data))

	// 写入 .log：length-prefix + data
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, length)
	if _, err := s.logFile.Write(lenBuf); err != nil {
		return fmt.Errorf("write length to log: %w", err)
	}
	if _, err := s.logFile.Write(data); err != nil {
		return fmt.Errorf("write data to log: %w", err)
	}

	// 写入 .index：offset(8) + position(8)
	pos := s.position
	idxBuf := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint64(idxBuf[0:8], uint64(msg.Offset))
	binary.BigEndian.PutUint64(idxBuf[8:16], uint64(pos))
	if _, err := s.indexFile.Write(idxBuf); err != nil {
		return fmt.Errorf("write index: %w", err)
	}

	// 更新内存状态
	s.position += 4 + int64(length)
	s.size += 4 + int64(length)
	s.numMsgs++
	return nil
}

// Read 读取指定 offset 的单条消息
//
// 先查 .index 找到文件位置，再从 .log 读取。
// 如果 offset 不在本 segment 范围内，返回 nil。
func (s *Segment) Read(offset int64) (*message.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if offset < s.baseOffset || offset >= s.baseOffset+int64(s.numMsgs) {
		return nil, nil
	}
	idx := int(offset - s.baseOffset)
	return s.readAtIndex(idx)
}

// ReadRange 从指定 offset 开始读取最多 maxCount 条
//
// 常用于顺序消费场景：一次拉取一批消息。
func (s *Segment) ReadRange(offset int64, maxCount int) ([]*message.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if offset < s.baseOffset || maxCount <= 0 || s.numMsgs == 0 {
		return nil, nil
	}
	startIdx := int(offset - s.baseOffset)
	if startIdx < 0 || startIdx >= s.numMsgs {
		return nil, nil
	}
	endIdx := startIdx + maxCount
	if endIdx > s.numMsgs {
		endIdx = s.numMsgs
	}

	result := make([]*message.Message, 0, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		msg, err := s.readAtIndex(i)
		if err != nil {
			return nil, err
		}
		result = append(result, msg)
	}
	return result, nil
}

// readAtIndex 读取 index 中第 idx 条消息（0-based）
//
// 读取 .index 文件对应位置得到 position，再从 .log 的 position 处读取消息。
func (s *Segment) readAtIndex(idx int) (*message.Message, error) {
	indexOffset := int64(idx * indexEntrySize)
	buf := make([]byte, indexEntrySize)
	if _, err := s.indexFile.ReadAt(buf, indexOffset); err != nil {
		return nil, fmt.Errorf("read index at %d: %w", indexOffset, err)
	}
	// 前 8 字节是 offset（可校验），后 8 字节是 position
	pos := int64(binary.BigEndian.Uint64(buf[8:16]))
	return s.readMessageAt(pos)
}

// readMessageAt 从 .log 文件的指定位置读取一条消息
//
// 先读 4 字节 length-prefix，再读对应长度的 JSON 数据，最后反序列化。
func (s *Segment) readMessageAt(pos int64) (*message.Message, error) {
	lenBuf := make([]byte, 4)
	if _, err := s.logFile.ReadAt(lenBuf, pos); err != nil {
		return nil, fmt.Errorf("read length at %d: %w", pos, err)
	}
	length := binary.BigEndian.Uint32(lenBuf)

	dataBuf := make([]byte, length)
	if _, err := s.logFile.ReadAt(dataBuf, pos+4); err != nil {
		return nil, fmt.Errorf("read data at %d: %w", pos+4, err)
	}

	var msg message.Message
	if err := json.Unmarshal(dataBuf, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}
	return &msg, nil
}

// LastMessageTime 返回该 segment 最后一条消息的时间戳
//
// 用于日志保留（log retention）判断：如果最后一条消息都已过期，
// 整个 segment 可以被安全删除。
func (s *Segment) LastMessageTime() (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.numMsgs == 0 {
		return time.Time{}, nil
	}
	msg, err := s.readAtIndex(s.numMsgs - 1)
	if err != nil {
		return time.Time{}, err
	}
	return msg.Timestamp, nil
}

// LogPath 返回 .log 文件路径
func (s *Segment) LogPath() string {
	return s.logPath
}

// IndexPath 返回 .index 文件路径
func (s *Segment) IndexPath() string {
	return s.indexPath
}

// Close 关闭文件句柄
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err1, err2 error
	if s.logFile != nil {
		err1 = s.logFile.Close()
	}
	if s.indexFile != nil {
		err2 = s.indexFile.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// parseBaseOffset 从 segment 文件名解析起始 offset
//
// 文件名格式：00000000000000000000.log
func parseBaseOffset(name string) (int64, error) {
	name = strings.TrimSuffix(name, ".log")
	name = strings.TrimSuffix(name, ".index")
	return strconv.ParseInt(name, 10, 64)
}
