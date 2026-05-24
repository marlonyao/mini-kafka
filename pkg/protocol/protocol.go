// Package protocol — Mini-Kafka 网络协议
//
// 定义 Broker 和 Client 之间的通信协议。
//
// 协议设计原则：
//   - 简单的二进制协议，基于 TCP 长连接
//   - 请求/响应模式：每个请求都有对应的响应
//   - 编码格式：4字节总长度 + 1字节类型 + JSON payload
//
// 数据帧格式：
//
//	┌──────────────┬──────────┬─────────────────┐
//	│ Length (4B)   │ Type (1B)│ Payload (N bytes)│
//	│ uint32 BE     │ uint8    │ JSON            │
//	└──────────────┴──────────┴─────────────────┘
//
// 类比：就像 HTTP 的 request/response，但是二进制的，更紧凑高效。
// 真正的 Kafka 用的是自定义二进制协议，这里简化为 JSON 方便学习。
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

// RequestType 请求类型
type RequestType uint8

const (
	// ProduceRequest 生产消息请求
	ProduceRequest RequestType = iota + 1
	// FetchRequest 拉取消息请求
	FetchRequest
	// CreateTopicRequest 创建 Topic 请求
	CreateTopicRequest
	// JoinGroupRequest 加入消费者组请求
	JoinGroupRequest
	// LeaveGroupRequest 离开消费者组请求
	LeaveGroupRequest
	// CommitOffsetRequest 提交 offset 请求
	CommitOffsetRequest
	// FetchOffsetRequest 获取 offset 请求
	FetchOffsetRequest
)

// ResponseType 响应状态
type ResponseType uint8

const (
	// ResponseOK 成功
	ResponseOK ResponseType = 0
	// ResponseError 错误
	ResponseError ResponseType = 1
)

// ─── 请求结构体 ──────────────────────────────────

// ProduceRequestData 生产消息请求
type ProduceRequestData struct {
	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Partition int    `json:"partition,omitempty"` // -1 表示自动选择
}

// FetchRequestData 拉取消息请求
type FetchRequestData struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	MaxCount  int    `json:"max_count"`
}

// CreateTopicRequestData 创建 Topic 请求
type CreateTopicRequestData struct {
	Name          string `json:"name"`
	NumPartitions int    `json:"num_partitions"`
}

// JoinGroupRequestData 加入消费者组请求
type JoinGroupRequestData struct {
	GroupID string `json:"group_id"`
	Topic   string `json:"topic"`
	Addr    string `json:"addr"` // 消费者的网络地址
}

// LeaveGroupRequestData 离开消费者组请求
type LeaveGroupRequestData struct {
	GroupID string `json:"group_id"`
	Addr    string `json:"addr"`
}

// CommitOffsetRequestData 提交 offset 请求
type CommitOffsetRequestData struct {
	GroupID   string `json:"group_id"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// FetchOffsetRequestData 获取 offset 请求
type FetchOffsetRequestData struct {
	GroupID   string `json:"group_id"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

// ─── 响应结构体 ──────────────────────────────────

// Response 通用响应
type Response struct {
	Status  ResponseType `json:"status"`
	Message string       `json:"message,omitempty"` // 错误信息
	Data    []byte       `json:"data,omitempty"`    // 附带数据（JSON 编码的业务数据）
}

// ProduceResult 生产消息结果
type ProduceResult struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

// FetchResult 拉取消息结果
type FetchResult struct {
	Messages []FetchedMessage `json:"messages"`
}

// FetchedMessage 拉取到的消息
type FetchedMessage struct {
	Offset    int64  `json:"offset"`
	Partition int    `json:"partition"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// JoinGroupResult 加入消费者组结果
type JoinGroupResult struct {
	AssignedPartitions []int `json:"assigned_partitions"`
}

// FetchOffsetResult 获取 offset 结果
type FetchOffsetResult struct {
	Offset int64 `json:"offset"`
}

// ─── 编解码 ──────────────────────────────────────

// WriteRequest 写一个请求到连接
//
// 编码格式：4字节长度 + 1字节类型 + JSON payload
func WriteRequest(conn net.Conn, reqType RequestType, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	// 总长度 = 1(类型) + len(data)
	totalLen := uint32(1 + len(data))
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, totalLen)

	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := conn.Write([]byte{byte(reqType)}); err != nil {
		return fmt.Errorf("write type: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

// ReadRequest 从连接读取一个请求
func ReadRequest(conn net.Conn) (RequestType, []byte, error) {
	// 读长度
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return 0, nil, err
	}
	totalLen := binary.BigEndian.Uint32(lenBuf)
	if totalLen < 1 {
		return 0, nil, fmt.Errorf("invalid request length: %d", totalLen)
	}

	// 读剩余数据（类型 + payload）
	buf := make([]byte, totalLen)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return 0, nil, err
	}

	reqType := RequestType(buf[0])
	payload := buf[1:]
	return reqType, payload, nil
}

// WriteResponse 写一个响应到连接
func WriteResponse(conn net.Conn, resp *Response) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}

// ReadResponse 从连接读取一个响应
func ReadResponse(conn net.Conn) (*Response, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, err
	}
	totalLen := binary.BigEndian.Uint32(lenBuf)

	buf := make([]byte, totalLen)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}

	var resp Response
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &resp, nil
}
