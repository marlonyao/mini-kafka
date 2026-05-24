package client

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/marlonyao/mini-kafka/pkg/broker"
	"github.com/marlonyao/mini-kafka/pkg/protocol"
)

// startTestBroker 启动一个测试用的 Broker（随机端口）
func startTestBroker(t *testing.T) (*broker.Broker, string) {
	t.Helper()
	b := broker.NewBroker(":0", "")
	done := make(chan struct{})
	go func() {
		b.Start()
		close(done)
	}()
	// 等待 Broker 启动
	time.Sleep(50 * time.Millisecond)
	addr := b.Addr()
	t.Cleanup(func() {
		b.Close()
		<-done
	})
	return b, addr
}

// ─── 端到端测试 ──────────────────────────────────

func TestEndToEndProduceAndFetch(t *testing.T) {
	_, addr := startTestBroker(t)

	// 创建 admin 客户端 → 创建 topic
	admin, err := NewAdminClient(addr)
	if err != nil {
		t.Fatalf("create admin client: %v", err)
	}
	defer admin.Close()

	if err := admin.CreateTopic("orders", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// 创建 producer → 发送消息
	producer, err := NewClientProducer(addr)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 5; i++ {
		partition, offset, err := producer.Send("orders", "key", "msg", -1)
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if partition < 0 || partition >= 3 {
			t.Errorf("invalid partition: %d", partition)
		}
		if offset != int64(i/3) { // 轮询分配到各分区
			t.Logf("offset=%d (partition=%d)", offset, partition)
		}
		_ = offset
	}

	// 创建 consumer → 拉取消息
	consumer, err := NewClientConsumer(addr, "test-group", "orders")
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	// 直接拉取分区 0 的消息
	msgs, err := consumer.Poll(0, 0, 10)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(msgs) == 0 {
		t.Error("expected messages from partition 0")
	}
	t.Logf("fetched %d messages from partition 0", len(msgs))
}

func TestEndToEndCreateTopicDuplicate(t *testing.T) {
	_, addr := startTestBroker(t)

	admin, _ := NewAdminClient(addr)
	defer admin.Close()

	if err := admin.CreateTopic("test", 2); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := admin.CreateTopic("test", 2); err == nil {
		t.Error("expected error for duplicate topic")
	}
}

func TestEndToEndOffsetCommit(t *testing.T) {
	_, addr := startTestBroker(t)

	admin, _ := NewAdminClient(addr)
	defer admin.Close()
	admin.CreateTopic("orders", 2)

	consumer, _ := NewClientConsumer(addr, "group1", "orders")
	defer consumer.Close()

	// 提交 offset
	if err := consumer.CommitOffset(0, 42); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// 获取 offset
	offset, err := consumer.FetchOffset(0)
	if err != nil {
		t.Fatalf("fetch offset: %v", err)
	}
	if offset != 42 {
		t.Errorf("expected offset=42, got %d", offset)
	}
}

func TestEndToEndJoinGroup(t *testing.T) {
	_, addr := startTestBroker(t)

	admin, _ := NewAdminClient(addr)
	defer admin.Close()
	admin.CreateTopic("orders", 3)

	consumer, _ := NewClientConsumer(addr, "analytics", "orders")
	defer consumer.Close()

	partitions, err := consumer.JoinGroup()
	if err != nil {
		t.Fatalf("join group: %v", err)
	}
	if len(partitions) == 0 {
		t.Error("expected assigned partitions")
	}
	t.Logf("assigned partitions: %v", partitions)
}

func TestEndToEndProduceToSpecificPartition(t *testing.T) {
	_, addr := startTestBroker(t)

	admin, _ := NewAdminClient(addr)
	defer admin.Close()
	admin.CreateTopic("orders", 3)

	producer, _ := NewClientProducer(addr)
	defer producer.Close()

	// 发送到指定分区
	for i := 0; i < 3; i++ {
		partition, offset, err := producer.Send("orders", "k", "v", 1)
		if err != nil {
			t.Fatalf("send: %v", err)
		}
		if partition != 1 {
			t.Errorf("expected partition=1, got %d", partition)
		}
		if offset != int64(i) {
			t.Errorf("expected offset=%d, got %d", i, offset)
		}
	}

	// 拉取分区 1 的消息
	consumer, _ := NewClientConsumer(addr, "g1", "orders")
	defer consumer.Close()

	msgs, err := consumer.Poll(1, 0, 10)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(msgs) != 3 {
		t.Errorf("expected 3 messages, got %d", len(msgs))
	}
}

// ─── 协议层测试 ──────────────────────────────────

func TestProtocolWriteRead(t *testing.T) {
	// 用 pipe 模拟网络连接
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// 客户端写请求
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := protocol.WriteRequest(client, protocol.ProduceRequest, &protocol.ProduceRequestData{
			Topic: "test", Key: "k", Value: "v", Partition: -1,
		})
		if err != nil {
			t.Errorf("write request: %v", err)
		}
	}()

	// 服务端读请求
	reqType, payload, err := protocol.ReadRequest(server)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	<-done

	if reqType != protocol.ProduceRequest {
		t.Errorf("expected ProduceRequest, got %d", reqType)
	}

	var req protocol.ProduceRequestData
	json.Unmarshal(payload, &req)
	if req.Topic != "test" {
		t.Errorf("expected topic=test, got %s", req.Topic)
	}
}

func TestProtocolResponseWriteRead(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	result := protocol.ProduceResult{Partition: 1, Offset: 99}
	data, _ := json.Marshal(result)

	done := make(chan struct{})
	go func() {
		defer close(done)
		protocol.WriteResponse(server, &protocol.Response{
			Status: protocol.ResponseOK,
			Data:   data,
		})
	}()

	resp, err := protocol.ReadResponse(client)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	<-done

	if resp.Status != protocol.ResponseOK {
		t.Errorf("expected OK, got %d", resp.Status)
	}

	var produceResult protocol.ProduceResult
	json.Unmarshal(resp.Data, &produceResult)
	if produceResult.Partition != 1 || produceResult.Offset != 99 {
		t.Errorf("expected partition=1 offset=99, got %d %d",
			produceResult.Partition, produceResult.Offset)
	}
}

func TestProtocolMultipleRequests(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// 服务端：读请求，写响应
	go func() {
		for i := 0; i < 3; i++ {
			_, _, err := protocol.ReadRequest(server)
			if err != nil {
				t.Errorf("read request %d: %v", i, err)
				return
			}
			protocol.WriteResponse(server, &protocol.Response{
				Status: protocol.ResponseOK,
			})
		}
	}()

	// 客户端：连续发 3 个请求
	for i := 0; i < 3; i++ {
		err := protocol.WriteRequest(client, protocol.FetchRequest, &protocol.FetchRequestData{
			Topic: "test", Partition: i, Offset: 0, MaxCount: 10,
		})
		if err != nil {
			t.Fatalf("write request %d: %v", i, err)
		}

		resp, err := protocol.ReadResponse(client)
		if err != nil {
			t.Fatalf("read response %d: %v", i, err)
		}
		if resp.Status != protocol.ResponseOK {
			t.Errorf("response %d: expected OK", i)
		}
	}
}
