package broker

import (
	"encoding/json"
	"testing"

	"github.com/marlonyao/mini-kafka/pkg/protocol"
)

// ─── 协议编解码测试 ──────────────────────────────

func TestProduceRequestDataJSON(t *testing.T) {
	req := protocol.ProduceRequestData{
		Topic:     "orders",
		Key:       "user_1",
		Value:     "hello",
		Partition: -1,
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded protocol.ProduceRequestData
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Topic != "orders" {
		t.Errorf("expected topic=orders, got %s", decoded.Topic)
	}
	if decoded.Key != "user_1" {
		t.Errorf("expected key=user_1, got %s", decoded.Key)
	}
	if decoded.Partition != -1 {
		t.Errorf("expected partition=-1, got %d", decoded.Partition)
	}
}

func TestFetchRequestDataJSON(t *testing.T) {
	req := protocol.FetchRequestData{
		Topic:     "orders",
		Partition: 0,
		Offset:    5,
		MaxCount:  10,
	}
	data, _ := json.Marshal(req)

	var decoded protocol.FetchRequestData
	json.Unmarshal(data, &decoded)

	if decoded.Offset != 5 {
		t.Errorf("expected offset=5, got %d", decoded.Offset)
	}
	if decoded.MaxCount != 10 {
		t.Errorf("expected maxCount=10, got %d", decoded.MaxCount)
	}
}

func TestResponseJSON(t *testing.T) {
	// OK response with data
	result := protocol.ProduceResult{Partition: 1, Offset: 42}
	data, _ := json.Marshal(result)

	resp := &protocol.Response{
		Status: protocol.ResponseOK,
		Data:   data,
	}
	encoded, _ := json.Marshal(resp)

	var decoded protocol.Response
	json.Unmarshal(encoded, &decoded)

	if decoded.Status != protocol.ResponseOK {
		t.Errorf("expected status=OK, got %d", decoded.Status)
	}

	var produceResult protocol.ProduceResult
	json.Unmarshal(decoded.Data, &produceResult)
	if produceResult.Partition != 1 || produceResult.Offset != 42 {
		t.Errorf("expected partition=1 offset=42, got partition=%d offset=%d",
			produceResult.Partition, produceResult.Offset)
	}
}

func TestErrorResponse(t *testing.T) {
	resp := errorResponse("topic '%s' not found", "test")
	if resp.Status != protocol.ResponseError {
		t.Errorf("expected error status, got %d", resp.Status)
	}
	if resp.Message != "topic 'test' not found" {
		t.Errorf("unexpected message: %s", resp.Message)
	}
}

// ─── Broker 单元测试 ─────────────────────────────

func TestBrokerCreateTopic(t *testing.T) {
	b := NewBroker(":0", "")
	defer b.Close()

	// 模拟请求
	payload, _ := json.Marshal(protocol.CreateTopicRequestData{
		Name:          "test-topic",
		NumPartitions: 3,
	})

	resp := b.handleCreateTopic(payload)
	if resp.Status != protocol.ResponseOK {
		t.Fatalf("create topic failed: %s", resp.Message)
	}

	// 验证 topic 已创建
	b.mu.RLock()
	topic, ok := b.topics["test-topic"]
	b.mu.RUnlock()

	if !ok {
		t.Fatal("topic not found after creation")
	}
	if topic.Name() != "test-topic" {
		t.Errorf("expected name=test-topic, got %s", topic.Name())
	}
	if topic.NumPartitions() != 3 {
		t.Errorf("expected 3 partitions, got %d", topic.NumPartitions())
	}
}

func TestBrokerCreateTopicDuplicate(t *testing.T) {
	b := NewBroker(":0", "")
	defer b.Close()

	payload, _ := json.Marshal(protocol.CreateTopicRequestData{
		Name:          "dup",
		NumPartitions: 1,
	})
	b.handleCreateTopic(payload)

	// 再次创建同名 topic
	resp := b.handleCreateTopic(payload)
	if resp.Status != protocol.ResponseError {
		t.Error("expected error for duplicate topic")
	}
}

func TestBrokerProduceAndFetch(t *testing.T) {
	b := NewBroker(":0", "")
	defer b.Close()

	// 创建 topic
	payload, _ := json.Marshal(protocol.CreateTopicRequestData{
		Name: "orders", NumPartitions: 3,
	})
	b.handleCreateTopic(payload)

	// 生产 5 条消息
	for i := 0; i < 5; i++ {
		payload, _ = json.Marshal(protocol.ProduceRequestData{
			Topic: "orders",
			Key:   "key",
			Value: "msg",
		})
		resp := b.handleProduce(payload)
		if resp.Status != protocol.ResponseOK {
			t.Fatalf("produce %d failed: %s", i, resp.Message)
		}
	}

	// 从分区 0 拉取
	payload, _ = json.Marshal(protocol.FetchRequestData{
		Topic: "orders", Partition: 0, Offset: 0, MaxCount: 10,
	})
	resp := b.handleFetch(payload)
	if resp.Status != protocol.ResponseOK {
		t.Fatalf("fetch failed: %s", resp.Message)
	}

	var result protocol.FetchResult
	json.Unmarshal(resp.Data, &result)
	if len(result.Messages) == 0 {
		t.Error("expected messages, got none")
	}

	// 验证 offset 连续
	for i, m := range result.Messages {
		if m.Offset != int64(i) {
			t.Errorf("msg[%d]: expected offset=%d, got %d", i, i, m.Offset)
		}
	}
}

func TestBrokerProduceToNonexistentTopic(t *testing.T) {
	b := NewBroker(":0", "")
	defer b.Close()

	payload, _ := json.Marshal(protocol.ProduceRequestData{
		Topic: "no-such-topic", Key: "k", Value: "v",
	})
	resp := b.handleProduce(payload)
	if resp.Status != protocol.ResponseError {
		t.Error("expected error for nonexistent topic")
	}
}

func TestBrokerCommitAndFetchOffset(t *testing.T) {
	b := NewBroker(":0", "")
	defer b.Close()

	// 提交 offset
	payload, _ := json.Marshal(protocol.CommitOffsetRequestData{
		GroupID: "group1", Topic: "orders", Partition: 0, Offset: 42,
	})
	resp := b.handleCommitOffset(payload)
	if resp.Status != protocol.ResponseOK {
		t.Fatalf("commit failed: %s", resp.Message)
	}

	// 获取 offset
	payload, _ = json.Marshal(protocol.FetchOffsetRequestData{
		GroupID: "group1", Topic: "orders", Partition: 0,
	})
	resp = b.handleFetchOffset(payload)
	if resp.Status != protocol.ResponseOK {
		t.Fatalf("fetch offset failed: %s", resp.Message)
	}

	var result protocol.FetchOffsetResult
	json.Unmarshal(resp.Data, &result)
	if result.Offset != 42 {
		t.Errorf("expected offset=42, got %d", result.Offset)
	}
}

func TestBrokerFetchOffsetNoCommit(t *testing.T) {
	b := NewBroker(":0", "")
	defer b.Close()

	payload, _ := json.Marshal(protocol.FetchOffsetRequestData{
		GroupID: "new-group", Topic: "orders", Partition: 0,
	})
	resp := b.handleFetchOffset(payload)
	if resp.Status != protocol.ResponseOK {
		t.Fatalf("fetch offset failed: %s", resp.Message)
	}

	var result protocol.FetchOffsetResult
	json.Unmarshal(resp.Data, &result)
	if result.Offset != 0 {
		t.Errorf("expected offset=0 for no prior commit, got %d", result.Offset)
	}
}
