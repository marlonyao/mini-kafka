// Package client — Mini-Kafka 客户端
//
// 提供 Producer 和 Consumer 的网络客户端，通过 TCP 连接 Broker。
//
// 和 pkg/producer.go、pkg/consumer.go 的区别：
//   - 原来的 Producer/Consumer 直接操作 Topic 对象（进程内调用）
//   - 这里的 ClientProducer/ClientConsumer 通过网络请求 Broker
//
// 架构对比：
//
//	之前（库模式）：
//	  Producer ──直接调用──→ Topic ──→ Partition
//
//	现在（Client/Server 模式）：
//	  ClientProducer ──TCP──→ Broker ──→ Topic ──→ Partition
//
// 类比：就像 kafka-clients 库（Java）之于 Kafka Broker。
// 应用代码只需要知道 Broker 地址，不需要引用 Topic 对象。
package client

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/marlonyao/mini-kafka/pkg/protocol"
)

// Client Kafka 客户端（连接管理）
//
// 维护一个到 Broker 的 TCP 长连接。
// 所有请求通过同一个连接发送（连接复用）。
type Client struct {
	brokerAddr string   // Broker 地址
	conn       net.Conn // TCP 连接
	mu         sync.Mutex
}

// NewClient 创建客户端并连接 Broker
func NewClient(brokerAddr string) (*Client, error) {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to broker %s: %w", brokerAddr, err)
	}
	return &Client{
		brokerAddr: brokerAddr,
		conn:       conn,
	}, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// request 发送请求并接收响应（线程安全）
func (c *Client) request(reqType protocol.RequestType, payload interface{}) (*protocol.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := protocol.WriteRequest(c.conn, reqType, payload); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	resp, err := protocol.ReadResponse(c.conn)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return resp, nil
}

// ─── ClientProducer 网络生产者 ────────────────────

// ClientProducer 通过网络发送消息到 Broker
type ClientProducer struct {
	client *Client
}

// NewClientProducer 创建网络生产者
func NewClientProducer(brokerAddr string) (*ClientProducer, error) {
	client, err := NewClient(brokerAddr)
	if err != nil {
		return nil, err
	}
	return &ClientProducer{client: client}, nil
}

// Send 发送一条消息
//
// 返回分区号和 offset。
// partition=-1 表示自动选择分区（轮询）。
func (p *ClientProducer) Send(topic, key, value string, partition int) (int, int64, error) {
	resp, err := p.client.request(protocol.ProduceRequest, &protocol.ProduceRequestData{
		Topic:     topic,
		Key:       key,
		Value:     value,
		Partition: partition,
	})
	if err != nil {
		return 0, 0, err
	}
	if resp.Status != protocol.ResponseOK {
		return 0, 0, fmt.Errorf("produce failed: %s", resp.Message)
	}

	var result protocol.ProduceResult
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return 0, 0, err
	}
	return result.Partition, result.Offset, nil
}

// Close 关闭生产者
func (p *ClientProducer) Close() error {
	return p.client.Close()
}

// ─── ClientConsumer 网络消费者 ────────────────────

// ClientConsumer 通过网络从 Broker 拉取消息
type ClientConsumer struct {
	client    *Client
	groupID   string
	topic     string
	localAddr string // 本地地址（用于 JoinGroup）
}

// NewClientConsumer 创建网络消费者
func NewClientConsumer(brokerAddr, groupID, topic string) (*ClientConsumer, error) {
	client, err := NewClient(brokerAddr)
	if err != nil {
		return nil, err
	}
	return &ClientConsumer{
		client:    client,
		groupID:   groupID,
		topic:     topic,
		localAddr: client.conn.LocalAddr().String(),
	}, nil
}

// JoinGroup 加入消费者组
//
// 返回分配到的分区列表。
// 对应 Kafka 的 JoinGroup + SyncGroup 两个请求（这里简化为一个）。
func (c *ClientConsumer) JoinGroup() ([]int, error) {
	resp, err := c.client.request(protocol.JoinGroupRequest, &protocol.JoinGroupRequestData{
		GroupID: c.groupID,
		Topic:   c.topic,
		Addr:    c.localAddr,
	})
	if err != nil {
		return nil, err
	}
	if resp.Status != protocol.ResponseOK {
		return nil, fmt.Errorf("join group failed: %s", resp.Message)
	}

	var result protocol.JoinGroupResult
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, err
	}
	return result.AssignedPartitions, nil
}

// LeaveGroup 离开消费者组
func (c *ClientConsumer) LeaveGroup() error {
	resp, err := c.client.request(protocol.LeaveGroupRequest, &protocol.LeaveGroupRequestData{
		GroupID: c.groupID,
		Addr:    c.localAddr,
	})
	if err != nil {
		return err
	}
	if resp.Status != protocol.ResponseOK {
		return fmt.Errorf("leave group failed: %s", resp.Message)
	}
	return nil
}

// Poll 从指定分区拉取消息
//
// 对应 Kafka 的 Fetch 请求。
// 这是"拉取模型"的核心：Consumer 主动拉，不是 Broker 推。
func (c *ClientConsumer) Poll(partition int, offset int64, maxCount int) ([]protocol.FetchedMessage, error) {
	resp, err := c.client.request(protocol.FetchRequest, &protocol.FetchRequestData{
		Topic:     c.topic,
		Partition: partition,
		Offset:    offset,
		MaxCount:  maxCount,
	})
	if err != nil {
		return nil, err
	}
	if resp.Status != protocol.ResponseOK {
		return nil, fmt.Errorf("fetch failed: %s", resp.Message)
	}

	var result protocol.FetchResult
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return nil, err
	}
	return result.Messages, nil
}

// CommitOffset 提交 offset
func (c *ClientConsumer) CommitOffset(partition int, offset int64) error {
	resp, err := c.client.request(protocol.CommitOffsetRequest, &protocol.CommitOffsetRequestData{
		GroupID:   c.groupID,
		Topic:     c.topic,
		Partition: partition,
		Offset:    offset,
	})
	if err != nil {
		return err
	}
	if resp.Status != protocol.ResponseOK {
		return fmt.Errorf("commit offset failed: %s", resp.Message)
	}
	return nil
}

// FetchOffset 获取已提交的 offset
func (c *ClientConsumer) FetchOffset(partition int) (int64, error) {
	resp, err := c.client.request(protocol.FetchOffsetRequest, &protocol.FetchOffsetRequestData{
		GroupID:   c.groupID,
		Topic:     c.topic,
		Partition: partition,
	})
	if err != nil {
		return 0, err
	}
	if resp.Status != protocol.ResponseOK {
		return 0, fmt.Errorf("fetch offset failed: %s", resp.Message)
	}

	var result protocol.FetchOffsetResult
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return 0, err
	}
	return result.Offset, nil
}

// Close 关闭消费者
func (c *ClientConsumer) Close() error {
	return c.client.Close()
}

// ─── Admin 客户端 ────────────────────────────────

// AdminClient 管理客户端（创建 Topic 等管理操作）
type AdminClient struct {
	client *Client
}

// NewAdminClient 创建管理客户端
func NewAdminClient(brokerAddr string) (*AdminClient, error) {
	client, err := NewClient(brokerAddr)
	if err != nil {
		return nil, err
	}
	return &AdminClient{client: client}, nil
}

// CreateTopic 创建 Topic
func (a *AdminClient) CreateTopic(name string, numPartitions int) error {
	resp, err := a.client.request(protocol.CreateTopicRequest, &protocol.CreateTopicRequestData{
		Name:          name,
		NumPartitions: numPartitions,
	})
	if err != nil {
		return err
	}
	if resp.Status != protocol.ResponseOK {
		return fmt.Errorf("create topic failed: %s", resp.Message)
	}
	return nil
}

// Close 关闭管理客户端
func (a *AdminClient) Close() error {
	return a.client.Close()
}
