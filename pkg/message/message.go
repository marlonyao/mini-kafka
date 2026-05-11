// Package message — Kafka 消息模型
//
// Kafka 消息的核心结构：
//
//	Record = Key + Value + Timestamp + Headers
//	Message = Record + Offset + Partition
//
// Record 是生产者发送的原始数据
// Message 是存储在分区中的完整数据（多了 offset 和 partition 信息）
package message

import "time"
//
// 对应 Kafka 的 ProducerRecord。
// Key 用于分区路由，Value 是实际数据。
type Record struct {
	Key       string            // 消息键（可选），用于分区路由
	Value     []byte            // 消息值（必需），实际的数据载荷
	Headers   map[string]string // 自定义头信息
	Timestamp time.Time         // 时间戳
}

// NewRecord 创建一条新记录
func NewRecord(key string, value []byte) Record {
	return Record{
		Key:       key,
		Value:     value,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}
}

// NewRecordWithString 用字符串值创建记录（便利方法）
func NewRecordWithString(key, value string) Record {
	return NewRecord(key, []byte(value))
}

// Message 存储在分区中的完整消息
//
// 和 Record 的区别：Message 包含了 offset 和 partition 信息。
// 对应 Kafka 内部的消息格式。
type Message struct {
	Offset    int64     // 在分区内的位移（从 0 开始递增）
	Partition int       // 所在分区号
	Key       string    // 消息键
	Value     []byte    // 消息值
	Headers   map[string]string
	Timestamp time.Time
}

// NewMessage 从 Record 创建 Message
func NewMessage(offset int64, partition int, record Record) Message {
	return Message{
		Offset:    offset,
		Partition: partition,
		Key:       record.Key,
		Value:     record.Value,
		Headers:   record.Headers,
		Timestamp: record.Timestamp,
	}
}
