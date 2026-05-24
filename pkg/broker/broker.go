// Package broker — Mini-Kafka Broker 服务端
//
// Broker 是 Mini-Kafka 的服务端，监听 TCP 端口，接受 Client 的连接和请求。
//
// 核心职责：
//   - 管理 Topic 和 Partition（消息存储）
//   - 处理 Producer 的生产请求
//   - 处理 Consumer 的拉取请求
//   - 管理 Consumer Group 和 Rebalance
//   - 管理 Offset 提交
//
// 架构：
//
//	Client (Producer) ──TCP──→ Broker ──→ Topic ──→ Partition ──→ Segment
//	Client (Consumer) ←──TCP── Broker ←── Topic ←── Partition ←── Segment
//
// 类比：Broker 就是 Kafka 集群中的一个节点。
// 真正的 Kafka Broker 还要处理副本同步、Controller 选举等，
// 这里简化为单节点模式。
package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	mini "github.com/marlonyao/mini-kafka/pkg"
	"github.com/marlonyao/mini-kafka/pkg/message"
	"github.com/marlonyao/mini-kafka/pkg/protocol"
)

// Broker Kafka 服务端
type Broker struct {
	addr      string                        // 监听地址，如 ":9092"
	listener  net.Listener                  // TCP 监听器
	topics    map[string]*mini.Topic        // topic 名称 → Topic 对象
	offsets   map[string]*mini.ConsumerOffset // group:topic → offset 存储
	groups    map[string]*consumerGroupEntry  // groupID → 消费者组信息
	dataDir   string                       // 数据持久化目录
	mu        sync.RWMutex
	quit      chan struct{}
}

// consumerGroupEntry 消费者组条目
type consumerGroupEntry struct {
	topic     string
	consumers map[string]bool // 消费者地址集合
}

// NewBroker 创建 Broker
//
// addr: 监听地址，如 ":9092"
// dataDir: 数据持久化目录
func NewBroker(addr string, dataDir string) *Broker {
	return &Broker{
		addr:    addr,
		topics:  make(map[string]*mini.Topic),
		offsets: make(map[string]*mini.ConsumerOffset),
		groups:  make(map[string]*consumerGroupEntry),
		dataDir: dataDir,
		quit:    make(chan struct{}),
	}
}

// Start 启动 Broker（阻塞运行）
func (b *Broker) Start() error {
	// 如果配置了 dataDir，启动时恢复已有 topic
	if b.dataDir != "" {
		if err := b.loadTopicsFromDisk(); err != nil {
			log.Printf("[Broker] warning: load topics from disk failed: %v", err)
		}
	}

	var err error
	b.listener, err = net.Listen("tcp", b.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", b.addr, err)
	}
	log.Printf("[Broker] listening on %s", b.addr)

	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.quit:
				return nil // 正常关闭
			default:
				log.Printf("[Broker] accept error: %v", err)
				continue
			}
		}
		go b.handleConn(conn)
	}
}

// Close 关闭 Broker
func (b *Broker) Close() {
	close(b.quit)
	if b.listener != nil {
		b.listener.Close()
	}
	log.Printf("[Broker] stopped")
}

// Addr 返回实际监听地址（端口为 0 时可获取实际端口）
func (b *Broker) Addr() string {
	if b.listener != nil {
		return b.listener.Addr().String()
	}
	return b.addr
}

// handleConn 处理一个客户端连接
//
// 每个连接一个 goroutine，循环读取请求并处理。
// 这就是"长连接"模式——一个连接可以发多个请求。
func (b *Broker) handleConn(conn net.Conn) {
	defer conn.Close()
	remoteAddr := conn.RemoteAddr().String()
	log.Printf("[Broker] new connection from %s", remoteAddr)

	// 跟踪该连接加入的消费者组，断开时自动清理
	var joinedGroups []struct {
		groupID string
		addr     string
	}

	defer func() {
		// 连接断开时，自动从所有消费者组中移除
		for _, g := range joinedGroups {
			b.removeConsumerFromGroup(g.groupID, g.addr)
		}
	}()

	for {
		reqType, payload, err := protocol.ReadRequest(conn)
		if err != nil {
			log.Printf("[Broker] connection %s closed: %v", remoteAddr, err)
			return
		}

		resp := b.handleRequest(reqType, payload)

		// 记录 JoinGroup，用于断开时自动清理
		if reqType == protocol.JoinGroupRequest && resp.Status == protocol.ResponseOK {
			var req protocol.JoinGroupRequestData
			if json.Unmarshal(payload, &req) == nil {
				joinedGroups = append(joinedGroups, struct {
					groupID string
					addr     string
				}{groupID: req.GroupID, addr: req.Addr})
			}
		}

		if err := protocol.WriteResponse(conn, resp); err != nil {
			log.Printf("[Broker] write response to %s: %v", remoteAddr, err)
			return
		}
	}
}

// handleRequest 路由请求到对应的处理函数
func (b *Broker) handleRequest(reqType protocol.RequestType, payload []byte) *protocol.Response {
	switch reqType {
	case protocol.ProduceRequest:
		return b.handleProduce(payload)
	case protocol.FetchRequest:
		return b.handleFetch(payload)
	case protocol.CreateTopicRequest:
		return b.handleCreateTopic(payload)
	case protocol.JoinGroupRequest:
		return b.handleJoinGroup(payload)
	case protocol.LeaveGroupRequest:
		return b.handleLeaveGroup(payload)
	case protocol.CommitOffsetRequest:
		return b.handleCommitOffset(payload)
	case protocol.FetchOffsetRequest:
		return b.handleFetchOffset(payload)
	default:
		return &protocol.Response{
			Status:  protocol.ResponseError,
			Message: fmt.Sprintf("unknown request type: %d", reqType),
		}
	}
}

// ─── 请求处理函数 ────────────────────────────────

// handleProduce 处理生产消息请求
func (b *Broker) handleProduce(payload []byte) *protocol.Response {
	var req protocol.ProduceRequestData
	if err := json.Unmarshal(payload, &req); err != nil {
		return errorResponse("invalid produce request: %v", err)
	}

	b.mu.RLock()
	topic, ok := b.topics[req.Topic]
	b.mu.RUnlock()

	if !ok {
		return errorResponse("topic '%s' not found", req.Topic)
	}

	record := message.NewRecordWithString(req.Key, req.Value)
	var msg *message.Message
	var err error
	if req.Partition >= 0 {
		msg, err = topic.Send(record, req.Partition)
	} else {
		msg, err = topic.Send(record)
	}
	if err != nil {
		return errorResponse("produce failed: %v", err)
	}

	result := protocol.ProduceResult{
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
	data, _ := json.Marshal(result)
	return okResponse(data)
}

// handleFetch 处理拉取消息请求
func (b *Broker) handleFetch(payload []byte) *protocol.Response {
	var req protocol.FetchRequestData
	if err := json.Unmarshal(payload, &req); err != nil {
		return errorResponse("invalid fetch request: %v", err)
	}

	b.mu.RLock()
	topic, ok := b.topics[req.Topic]
	b.mu.RUnlock()

	if !ok {
		return errorResponse("topic '%s' not found", req.Topic)
	}

	part, err := topic.Partition(req.Partition)
	if err != nil {
		return errorResponse("partition error: %v", err)
	}

	msgs := part.Read(req.Offset, req.MaxCount)
	fetched := make([]protocol.FetchedMessage, 0, len(msgs))
	for _, m := range msgs {
		fetched = append(fetched, protocol.FetchedMessage{
			Offset:    m.Offset,
			Partition: m.Partition,
			Key:       m.Key,
			Value:     string(m.Value),
		})
	}

	result := protocol.FetchResult{Messages: fetched}
	data, _ := json.Marshal(result)
	return okResponse(data)
}

// handleCreateTopic 处理创建 Topic 请求
func (b *Broker) handleCreateTopic(payload []byte) *protocol.Response {
	var req protocol.CreateTopicRequestData
	if err := json.Unmarshal(payload, &req); err != nil {
		return errorResponse("invalid create topic request: %v", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[req.Name]; ok {
		return errorResponse("topic '%s' already exists", req.Name)
	}

	var topic *mini.Topic
	var err error
	if b.dataDir != "" {
		topic, err = mini.NewTopicWithDir(req.Name, req.NumPartitions, b.dataDir)
		if err != nil {
			return errorResponse("create topic '%s' failed: %v", req.Name, err)
		}
	} else {
		topic = mini.NewTopic(req.Name, req.NumPartitions)
	}

	b.topics[req.Name] = topic
	log.Printf("[Broker] created topic '%s' with %d partitions", req.Name, req.NumPartitions)

	return okResponse(nil)
}

// handleJoinGroup 处理加入消费者组请求
func (b *Broker) handleJoinGroup(payload []byte) *protocol.Response {
	var req protocol.JoinGroupRequestData
	if err := json.Unmarshal(payload, &req); err != nil {
		return errorResponse("invalid join group request: %v", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// 确保 topic 存在
	topic, ok := b.topics[req.Topic]
	if !ok {
		return errorResponse("topic '%s' not found", req.Topic)
	}

	// 确保 offset 存储存在
	key := req.GroupID + ":" + req.Topic
	if b.offsets[key] == nil {
		b.offsets[key] = mini.NewConsumerOffset()
	}

	// 确保消费者组存在
	if b.groups[req.GroupID] == nil {
		b.groups[req.GroupID] = &consumerGroupEntry{
			topic:     req.Topic,
			consumers: make(map[string]bool),
		}
	}

	// 添加消费者
	b.groups[req.GroupID].consumers[req.Addr] = true

	// 重新计算分区分配（Range Assignor）
	assigned := b.assignPartitions(req.GroupID, topic)

	log.Printf("[Broker] consumer %s joined group '%s', assigned partitions %v",
		req.Addr, req.GroupID, assigned)

	result := protocol.JoinGroupResult{AssignedPartitions: assigned}
	data, _ := json.Marshal(result)
	return okResponse(data)
}

// handleLeaveGroup 处理离开消费者组请求
func (b *Broker) handleLeaveGroup(payload []byte) *protocol.Response {
	var req protocol.LeaveGroupRequestData
	if err := json.Unmarshal(payload, &req); err != nil {
		return errorResponse("invalid leave group request: %v", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	group, ok := b.groups[req.GroupID]
	if !ok {
		return okResponse(nil)
	}

	delete(group.consumers, req.Addr)
	log.Printf("[Broker] consumer %s left group '%s'", req.Addr, req.GroupID)

	// 如果组里还有人，触发 reassign
	topic, tok := b.topics[group.topic]
	if tok && len(group.consumers) > 0 {
		for addr := range group.consumers {
			assigned := b.assignPartitions(req.GroupID, topic)
			log.Printf("[Broker] rebalance: consumer %s now has partitions %v", addr, assigned)
		}
	}

	return okResponse(nil)
}

// handleCommitOffset 处理提交 offset 请求
func (b *Broker) handleCommitOffset(payload []byte) *protocol.Response {
	var req protocol.CommitOffsetRequestData
	if err := json.Unmarshal(payload, &req); err != nil {
		return errorResponse("invalid commit offset request: %v", err)
	}

	b.mu.Lock()
	key := req.GroupID + ":" + req.Topic
	if b.offsets[key] == nil {
		b.offsets[key] = mini.NewConsumerOffset()
	}
	co := b.offsets[key]
	b.mu.Unlock()

	co.Commit(req.GroupID, req.Topic, req.Partition, req.Offset)
	log.Printf("[Broker] committed offset: group=%s, topic=%s, partition=%d, offset=%d",
		req.GroupID, req.Topic, req.Partition, req.Offset)

	return okResponse(nil)
}

// handleFetchOffset 处理获取 offset 请求
func (b *Broker) handleFetchOffset(payload []byte) *protocol.Response {
	var req protocol.FetchOffsetRequestData
	if err := json.Unmarshal(payload, &req); err != nil {
		return errorResponse("invalid fetch offset request: %v", err)
	}

	b.mu.RLock()
	key := req.GroupID + ":" + req.Topic
	co := b.offsets[key]
	b.mu.RUnlock()

	var offset int64
	if co != nil {
		offset = co.Get(req.GroupID, req.Topic, req.Partition)
	}

	result := protocol.FetchOffsetResult{Offset: offset}
	data, _ := json.Marshal(result)
	return okResponse(data)
}

// ─── 辅助方法 ────────────────────────────────────

// loadTopicsFromDisk 从 dataDir 恢复已有 topic
//
// 目录结构: dataDir/<topicName>/partition-<N>/
func (b *Broker) loadTopicsFromDisk() error {
	entries, err := os.ReadDir(b.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 目录不存在，首次启动
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		topicName := entry.Name()

		// 扫描 topic 目录下的 partition-* 子目录来确定分区数
		partEntries, err := os.ReadDir(filepath.Join(b.dataDir, topicName))
		if err != nil {
			continue
		}

		maxPartition := -1
		for _, pe := range partEntries {
			if pe.IsDir() {
				var pID int
				if _, err := fmt.Sscanf(pe.Name(), "partition-%d", &pID); err == nil {
					if pID > maxPartition {
						maxPartition = pID
					}
			}
		}
		}

		if maxPartition < 0 {
			continue // 没有分区目录
		}

		numPartitions := maxPartition + 1
		topic, err := mini.NewTopicWithDir(topicName, numPartitions, b.dataDir)
		if err != nil {
			log.Printf("[Broker] warning: restore topic '%s' failed: %v", topicName, err)
			continue
		}

		b.topics[topicName] = topic
		log.Printf("[Broker] restored topic '%s' (%d partitions) from disk", topicName, numPartitions)
	}

	return nil
}

// removeConsumerFromGroup 从消费者组中移除消费者（连接断开时调用）
func (b *Broker) removeConsumerFromGroup(groupID, addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	group, ok := b.groups[groupID]
	if !ok {
		log.Printf("[Broker] cleanup: group '%s' not found for consumer %s", groupID, addr)
		return
	}

	delete(group.consumers, addr)
	log.Printf("[Broker] consumer %s disconnected, removed from group '%s' (remaining: %d)",
		addr, groupID, len(group.consumers))

	if len(group.consumers) == 0 {
		delete(b.groups, groupID)
		log.Printf("[Broker] group '%s' is empty, cleaned up", groupID)
	}
}

// assignPartitions 分配分区给消费者组（Range Assignor）
//
// 返回给新加入的消费者的分区列表。
// 简化实现：返回按顺序分配的分区。
func (b *Broker) assignPartitions(groupID string, topic *mini.Topic) []int {
	group := b.groups[groupID]
	if group == nil {
		return nil
	}

	// 收集所有消费者地址并排序（保证分配稳定）
	addrs := make([]string, 0, len(group.consumers))
	for addr := range group.consumers {
		addrs = append(addrs, addr)
	}

	// Range Assignor: 分区 i 分配给消费者 i % numConsumers
	numPartitions := topic.NumPartitions()
	numConsumers := len(addrs)

	result := make([]int, 0)
	for pid := 0; pid < numPartitions; pid++ {
		if addrs[pid%numConsumers] == lastAddr(group.consumers) {
			result = append(result, pid)
		}
	}

	return result
}

// lastAddr 获取消费者组中最后加入的消费者地址
func lastAddr(consumers map[string]bool) string {
	var last string
	for addr := range consumers {
		last = addr
	}
	return last
}

// okResponse 构造成功响应
func okResponse(data []byte) *protocol.Response {
	return &protocol.Response{
		Status: protocol.ResponseOK,
		Data:   data,
	}
}

// errorResponse 构造错误响应
func errorResponse(format string, args ...interface{}) *protocol.Response {
	return &protocol.Response{
		Status:  protocol.ResponseError,
		Message: fmt.Sprintf(format, args...),
	}
}
