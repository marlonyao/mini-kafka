package mini_kafka

import (
	"testing"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

// ─── Record 测试 ──────────────────────────────────

func TestNewRecord(t *testing.T) {
	r := message.NewRecordWithString("user1", "login")
	if r.Key != "user1" {
		t.Errorf("expected key=user1, got %s", r.Key)
	}
	if string(r.Value) != "login" {
		t.Errorf("expected value=login, got %s", r.Value)
	}
	if r.Timestamp.IsZero() {
		t.Error("timestamp should not be zero")
	}
}

func TestRecordWithHeaders(t *testing.T) {
	r := message.NewRecordWithString("k", "v")
	r.Headers["trace_id"] = "abc123"
	if r.Headers["trace_id"] != "abc123" {
		t.Errorf("expected header trace_id=abc123, got %s", r.Headers["trace_id"])
	}
}

func TestRecordNoKey(t *testing.T) {
	r := message.NewRecord("", []byte("no key"))
	if r.Key != "" {
		t.Errorf("expected empty key, got %s", r.Key)
	}
}

// ─── Partition 测试 ───────────────────────────────

func TestEmptyPartition(t *testing.T) {
	p := NewPartition(0)
	if p.Size() != 0 {
		t.Errorf("expected size=0, got %d", p.Size())
	}
	if p.LatestOffset() != -1 {
		t.Errorf("expected latestOffset=-1, got %d", p.LatestOffset())
	}
}

func TestAppendSingle(t *testing.T) {
	p := NewPartition(0)
	msg := p.Append(message.NewRecordWithString("k1", "hello"))
	if msg.Offset != 0 {
		t.Errorf("expected offset=0, got %d", msg.Offset)
	}
	if msg.Partition != 0 {
		t.Errorf("expected partition=0, got %d", msg.Partition)
	}
	if msg.Key != "k1" {
		t.Errorf("expected key=k1, got %s", msg.Key)
	}
	if p.Size() != 1 {
		t.Errorf("expected size=1, got %d", p.Size())
	}
}

func TestAppendMultiple(t *testing.T) {
	p := NewPartition(0)
	p.Append(message.NewRecordWithString("", "a"))
	p.Append(message.NewRecordWithString("", "b"))
	p.Append(message.NewRecordWithString("", "c"))
	if p.Size() != 3 {
		t.Errorf("expected size=3, got %d", p.Size())
	}
	if p.LatestOffset() != 2 {
		t.Errorf("expected latestOffset=2, got %d", p.LatestOffset())
	}
}

func TestAppendBatch(t *testing.T) {
	p := NewPartition(0)
	records := []message.Record{
		message.NewRecordWithString("", "x"),
		message.NewRecordWithString("", "y"),
		message.NewRecordWithString("", "z"),
	}
	msgs := p.AppendBatch(records)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if msgs[0].Offset != 0 || msgs[1].Offset != 1 || msgs[2].Offset != 2 {
		t.Errorf("offsets should be [0,1,2], got [%d,%d,%d]",
			msgs[0].Offset, msgs[1].Offset, msgs[2].Offset)
	}
}

func TestReadRange(t *testing.T) {
	p := NewPartition(0)
	for i := 0; i < 10; i++ {
		p.Append(message.NewRecordWithString("", string(rune('a'+i))))
	}
	msgs := p.Read(3, 4)
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(msgs))
	}
	for i, m := range msgs {
		if m.Offset != int64(3+i) {
			t.Errorf("msg[%d]: expected offset=%d, got %d", i, 3+i, m.Offset)
		}
	}
}

func TestReadBeyondEnd(t *testing.T) {
	p := NewPartition(0)
	p.Append(message.NewRecordWithString("", "only"))
	msgs := p.Read(5, 10)
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages, got %d", len(msgs))
	}
}

func TestGetByOffset(t *testing.T) {
	p := NewPartition(0)
	p.Append(message.NewRecordWithString("", "a"))
	p.Append(message.NewRecordWithString("", "b"))
	p.Append(message.NewRecordWithString("", "c"))

	msg := p.Get(1)
	if msg == nil {
		t.Fatal("expected message at offset 1, got nil")
	}
	if string(msg.Value) != "b" {
		t.Errorf("expected value=b, got %s", msg.Value)
	}

	if p.Get(99) != nil {
		t.Error("expected nil for offset 99")
	}
}

func TestOffsetSequential(t *testing.T) {
	p := NewPartition(1)
	for i := 0; i < 5; i++ {
		msg := p.Append(message.NewRecordWithString("", "msg"))
		if msg.Offset != int64(i) {
			t.Errorf("expected offset=%d, got %d", i, msg.Offset)
		}
		if msg.Partition != 1 {
			t.Errorf("expected partition=1, got %d", msg.Partition)
		}
	}
}

func TestTruncate(t *testing.T) {
	p := NewPartition(0)
	for i := 0; i < 10; i++ {
		p.Append(message.NewRecordWithString("", string(rune('0'+i))))
	}
	deleted := p.Truncate(5)
	if deleted != 5 {
		t.Errorf("expected deleted=5, got %d", deleted)
	}
	if p.Size() != 5 {
		t.Errorf("expected size=5, got %d", p.Size())
	}
	// offset=5 的消息应该还在
	msg := p.Get(5)
	if msg == nil {
		t.Fatal("expected message at offset 5, got nil")
	}
	if string(msg.Value) != "5" {
		t.Errorf("expected value=5, got %s", msg.Value)
	}
	// offset=4 已被截断
	if p.Get(4) != nil {
		t.Error("offset 4 should be truncated")
	}
}

// ─── Topic 测试 ────────────────────────────────────

func TestCreateTopic(t *testing.T) {
	topic := NewTopic("test", 3)
	if topic.Name() != "test" {
		t.Errorf("expected name=test, got %s", topic.Name())
	}
	if topic.NumPartitions() != 3 {
		t.Errorf("expected 3 partitions, got %d", topic.NumPartitions())
	}
}

func TestSendRoundRobin(t *testing.T) {
	topic := NewTopic("test", 3)
	for i := 0; i < 6; i++ {
		topic.Send(message.NewRecordWithString("", "msg"))
	}
	// 每个分区 2 条
	for i := 0; i < 3; i++ {
		p, _ := topic.Partition(i)
		if p.Size() != 2 {
			t.Errorf("partition %d: expected size=2, got %d", i, p.Size())
		}
	}
}

func TestSendToSpecificPartition(t *testing.T) {
	topic := NewTopic("test", 3)
	topic.Send(message.NewRecordWithString("", "a"), 1)
	topic.Send(message.NewRecordWithString("", "b"), 1)

	p0, _ := topic.Partition(0)
	p1, _ := topic.Partition(1)
	p2, _ := topic.Partition(2)

	if p0.Size() != 0 || p2.Size() != 0 {
		t.Error("partition 0 and 2 should be empty")
	}
	if p1.Size() != 2 {
		t.Errorf("partition 1: expected size=2, got %d", p1.Size())
	}
}

func TestConsumeFromPartition(t *testing.T) {
	topic := NewTopic("orders", 2)
	topic.Send(message.NewRecordWithString("", "order1"), 0)
	topic.Send(message.NewRecordWithString("", "order2"), 0)
	topic.Send(message.NewRecordWithString("", "order3"), 1)

	msgsP0, _ := topic.Consume(0, 0, 100)
	msgsP1, _ := topic.Consume(1, 0, 100)

	if len(msgsP0) != 2 {
		t.Errorf("partition 0: expected 2 messages, got %d", len(msgsP0))
	}
	if len(msgsP1) != 1 {
		t.Errorf("partition 1: expected 1 message, got %d", len(msgsP1))
	}
}

func TestPartitionNotExist(t *testing.T) {
	topic := NewTopic("test", 2)
	_, err := topic.Partition(99)
	if err == nil {
		t.Error("expected error for non-existent partition")
	}
}

func TestTotalMessages(t *testing.T) {
	topic := NewTopic("test", 3)
	for i := 0; i < 9; i++ {
		topic.Send(message.NewRecordWithString("", "msg"))
	}
	if topic.TotalMessages() != 9 {
		t.Errorf("expected total=9, got %d", topic.TotalMessages())
	}
}
