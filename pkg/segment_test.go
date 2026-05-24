package mini_kafka

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marlonyao/mini-kafka/pkg/message"
)

// ─── Segment 基础操作测试 ─────────────────────────

func TestSegmentCreateAndPaths(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	defer seg.Close()

	expectedLog := filepath.Join(dir, "00000000000000000000.log")
	expectedIdx := filepath.Join(dir, "00000000000000000000.index")
	if seg.LogPath() != expectedLog {
		t.Errorf("log path: expected %s, got %s", expectedLog, seg.LogPath())
	}
	if seg.IndexPath() != expectedIdx {
		t.Errorf("index path: expected %s, got %s", expectedIdx, seg.IndexPath())
	}

	// 文件应该已经存在
	if _, err := os.Stat(expectedLog); os.IsNotExist(err) {
		t.Error("log file not created")
	}
	if _, err := os.Stat(expectedIdx); os.IsNotExist(err) {
		t.Error("index file not created")
	}
}

func TestSegmentBaseOffsetNaming(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 42, 1024*1024)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	defer seg.Close()

	expectedLog := filepath.Join(dir, "00000000000000000042.log")
	if seg.LogPath() != expectedLog {
		t.Errorf("expected %s, got %s", expectedLog, seg.LogPath())
	}
	if seg.BaseOffset() != 42 {
		t.Errorf("expected baseOffset=42, got %d", seg.BaseOffset())
	}
}

func TestSegmentAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	defer seg.Close()

	// 写入 3 条消息
	msgs := make([]*message.Message, 3)
	for i := 0; i < 3; i++ {
		msgs[i] = &message.Message{
			Offset:    int64(i),
			Partition: 0,
			Key:       "key",
			Value:     []byte("value-" + string(rune('a'+i))),
		}
		if err := seg.Append(msgs[i]); err != nil {
			t.Fatalf("append msg %d: %v", i, err)
		}
	}

	if seg.NumMessages() != 3 {
		t.Errorf("expected numMsgs=3, got %d", seg.NumMessages())
	}

	// 按偏移量读取
	for i := 0; i < 3; i++ {
		read, err := seg.Read(int64(i))
		if err != nil {
			t.Fatalf("read offset %d: %v", i, err)
		}
		if read == nil {
			t.Fatalf("expected message at offset %d, got nil", i)
		}
		if read.Offset != int64(i) {
			t.Errorf("offset %d: expected offset=%d, got %d", i, i, read.Offset)
		}
		if string(read.Value) != "value-"+string(rune('a'+i)) {
			t.Errorf("offset %d: expected value=%s, got %s", i, "value-"+string(rune('a'+i)), read.Value)
		}
	}
}

func TestSegmentReadOutOfRange(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	defer seg.Close()

	seg.Append(&message.Message{Offset: 0, Partition: 0, Value: []byte("a")})
	seg.Append(&message.Message{Offset: 1, Partition: 0, Value: []byte("b")})

	// offset 超出范围
	msg, err := seg.Read(99)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if msg != nil {
		t.Error("expected nil for out-of-range offset")
	}

	// 负 offset
	msg, err = seg.Read(-1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if msg != nil {
		t.Error("expected nil for negative offset")
	}
}

func TestSegmentReadRange(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	defer seg.Close()

	for i := 0; i < 10; i++ {
		seg.Append(&message.Message{
			Offset: int64(i), Partition: 0,
			Value: []byte{byte(i)},
		})
	}

	// 读取 [3, 7)
	msgs, err := seg.ReadRange(3, 4)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(msgs))
	}
	for i, m := range msgs {
		if m.Offset != int64(3+i) {
			t.Errorf("msg[%d]: expected offset=%d, got %d", i, 3+i, m.Offset)
		}
	}

	// 跨越边界的读取
	msgs, err = seg.ReadRange(8, 100)
	if err != nil {
		t.Fatalf("ReadRange beyond: %v", err)
	}
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages, got %d", len(msgs))
	}
}

func TestSegmentIsFull(t *testing.T) {
	dir := t.TempDir()
	// 极小的 segment，只能装几条消息
	seg, err := NewSegment(dir, 0, 100)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	defer seg.Close()

	if seg.IsFull() {
		t.Error("new segment should not be full")
	}

	// 持续写入直到满
	for i := 0; i < 100; i++ {
		seg.Append(&message.Message{
			Offset: int64(i), Partition: 0,
			Key: "k", Value: []byte("hello-world-data"),
		})
		if seg.IsFull() {
			break
		}
	}

	if !seg.IsFull() {
		t.Error("segment should be full after writing enough data")
	}
}

func TestSegmentContains(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 10, 1024*1024) // baseOffset=10
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	defer seg.Close()

	for i := 0; i < 5; i++ {
		seg.Append(&message.Message{
			Offset: int64(10 + i), Partition: 0, Value: []byte("x"),
		})
	}

	// baseOffset=10, numMsgs=5 → 包含 [10, 15)
	tests := []struct {
		offset   int64
		expected bool
	}{
		{9, false},
		{10, true},
		{12, true},
		{14, true},
		{15, false},
		{100, false},
	}
	for _, tt := range tests {
		got := seg.Contains(tt.offset)
		if got != tt.expected {
			t.Errorf("Contains(%d): expected %v, got %v", tt.offset, tt.expected, got)
		}
	}
}

// ─── Segment 恢复（OpenSegment）测试 ─────────────

func TestSegmentOpenRecover(t *testing.T) {
	dir := t.TempDir()
	seg, err := NewSegment(dir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}

	// 写入 5 条消息后关闭
	for i := 0; i < 5; i++ {
		seg.Append(&message.Message{
			Offset: int64(i), Partition: 0,
			Key: "key", Value: []byte("value"),
		})
	}
	seg.Close()

	// 重新打开
	seg2, err := OpenSegment(dir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	defer seg2.Close()

	// 验证恢复状态
	if seg2.NumMessages() != 5 {
		t.Errorf("expected numMsgs=5, got %d", seg2.NumMessages())
	}
	if seg2.BaseOffset() != 0 {
		t.Errorf("expected baseOffset=0, got %d", seg2.BaseOffset())
	}

	// 验证数据可读
	msg, err := seg2.Read(3)
	if err != nil {
		t.Fatalf("read after recover: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message at offset 3, got nil")
	}
	if string(msg.Value) != "value" {
		t.Errorf("expected value=value, got %s", msg.Value)
	}
}

// ─── Partition 持久化测试（步5核心） ──────────────

func TestPartitionPersistenceWithDir(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}

	// 写入 10 条消息
	for i := 0; i < 10; i++ {
		p.Append(message.NewRecordWithString("key", string(rune('0'+i))))
	}

	if p.Size() != 10 {
		t.Errorf("expected size=10, got %d", p.Size())
	}
	if p.LatestOffset() != 9 {
		t.Errorf("expected latestOffset=9, got %d", p.LatestOffset())
	}

	// 验证磁盘文件存在
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	logFiles := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logFiles++
		}
	}
	if logFiles == 0 {
		t.Error("no .log files found in data directory")
	}

	p.Close()
}

func TestPartitionRecoverFromDisk(t *testing.T) {
	dir := t.TempDir()

	// 第一阶段：写入数据
	p1, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}
	for i := 0; i < 20; i++ {
		p1.Append(message.NewRecordWithString("", string(rune('a'+i%26))))
	}
	nextOff := p1.NextOffset()
	p1.Close()

	// 第二阶段：重新打开，验证恢复
	p2, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("recover partition: %v", err)
	}
	defer p2.Close()

	if p2.Size() != 20 {
		t.Errorf("expected size=20 after recovery, got %d", p2.Size())
	}
	if p2.LatestOffset() != 19 {
		t.Errorf("expected latestOffset=19 after recovery, got %d", p2.LatestOffset())
	}
	if p2.NextOffset() != nextOff {
		t.Errorf("expected nextOffset=%d, got %d", nextOff, p2.NextOffset())
	}

	// 验证消息内容
	msg := p2.Get(5)
	if msg == nil {
		t.Fatal("expected message at offset 5, got nil")
	}
	if string(msg.Value) != string(rune('a'+5%26)) {
		t.Errorf("expected value=%c, got %s", 'a'+5%26, msg.Value)
	}

	// 恢复后继续追加
	p2.Append(message.NewRecordWithString("", "new-msg"))
	if p2.Size() != 21 {
		t.Errorf("expected size=21 after append, got %d", p2.Size())
	}
	if p2.LatestOffset() != 20 {
		t.Errorf("expected latestOffset=20, got %d", p2.LatestOffset())
	}
}

func TestPartitionSegmentRolling(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}
	defer p.Close()

	// 设置很小的 segment 大小，触发滚动
	p.SetMaxSegmentSize(200) // ~200 字节就滚动

	// 写入足够多的消息以触发多次滚动
	for i := 0; i < 50; i++ {
		p.Append(message.NewRecordWithString("key", "some-payload-data-here"))
	}

	// 检查磁盘上应该有多个 segment 文件
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	logFiles := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logFiles++
		}
	}
	if logFiles < 2 {
		t.Errorf("expected at least 2 segment files, got %d", logFiles)
	}

	// 数据完整性：offset 连续
	if p.Size() != 50 {
		t.Errorf("expected size=50, got %d", p.Size())
	}
	if p.LatestOffset() != 49 {
		t.Errorf("expected latestOffset=49, got %d", p.LatestOffset())
	}

	// 跨 segment 读取
	msgs := p.Read(0, 50)
	if len(msgs) != 50 {
		t.Errorf("expected 50 messages, got %d", len(msgs))
	}
	for i, m := range msgs {
		if m.Offset != int64(i) {
			t.Errorf("msg[%d]: expected offset=%d, got %d", i, i, m.Offset)
		}
	}

	// 跨 segment 单条读取
	msg := p.Get(25)
	if msg == nil {
		t.Fatal("expected message at offset 25, got nil")
	}
	if msg.Offset != 25 {
		t.Errorf("expected offset=25, got %d", msg.Offset)
	}
}

func TestPartitionReadAllAfterSegmentRolling(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}
	defer p.Close()

	p.SetMaxSegmentSize(150)

	for i := 0; i < 30; i++ {
		p.Append(message.NewRecordWithString("", string(rune('A'+i%26))))
	}

	allMsgs := p.ReadAll()
	if len(allMsgs) != 30 {
		t.Errorf("expected 30 messages, got %d", len(allMsgs))
	}
}

func TestPartitionCleanupExpiredSegments(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}
	defer p.CleanupData()

	p.SetMaxSegmentSize(150) // 小 segment，多滚动

	// 写入第一批（将成为旧 segment）
	for i := 0; i < 20; i++ {
		p.Append(message.NewRecordWithString("", "old-data"))
	}

	// 设置极短的保留时间并等待过期
	p.SetRetentionMs(1)
	time.Sleep(50 * time.Millisecond) // 等待旧 segment 过期

	// 恢复大 segment，写入新数据（保证成为活跃 segment）
	p.SetMaxSegmentSize(1024 * 1024)
	for i := 0; i < 5; i++ {
		p.Append(message.NewRecordWithString("", "new-data"))
	}

	// 触发清理
	deleted := p.Cleanup()
	if deleted == 0 {
		t.Error("expected some segments to be cleaned up, but got 0 deleted")
	}

	// 清理后消息数应该减少（可能不是精确 5 条，因为活跃 segment 可能包含部分旧数据）
	if p.Size() >= 20 {
		t.Errorf("expected size < 20 after cleanup, got %d", p.Size())
	}

	// 新数据应该还在
	msg := p.Get(p.LatestOffset())
	if msg == nil {
		t.Fatal("latest message should still exist after cleanup")
	}
	if string(msg.Value) != "new-data" {
		t.Errorf("expected value=new-data, got %s", msg.Value)
	}
}

func TestPartitionCleanupDoesNotRemoveActiveSegment(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}
	defer p.CleanupData()

	p.Append(message.NewRecordWithString("", "data"))
	p.SetRetentionMs(1) // 极短保留时间

	// 只有一个 segment（活跃 segment），cleanup 不应删除它
	deleted := p.Cleanup()
	if deleted != 0 {
		t.Error("active segment should not be cleaned up")
	}
	if p.Size() != 1 {
		t.Errorf("expected size=1, got %d", p.Size())
	}
}

func TestPartitionTruncateAndRead(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}
	defer p.Close()

	p.SetMaxSegmentSize(150) // 小 segment

	for i := 0; i < 30; i++ {
		p.Append(message.NewRecordWithString("", string(rune('0'+i%10))))
	}

	// 截断前 10 条
	deleted := p.Truncate(10)
	if deleted != 10 {
		t.Errorf("expected deleted=10, got %d", deleted)
	}
	if p.Size() != 20 {
		t.Errorf("expected size=20, got %d", p.Size())
	}

	// offset=9 应该不可读
	if p.Get(9) != nil {
		t.Error("offset 9 should be truncated")
	}
	// offset=10 应该可读
	msg := p.Get(10)
	if msg == nil {
		t.Fatal("offset 10 should be readable")
	}
}

func TestPartitionDataDir(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}
	defer p.Close()

	if p.DataDir() != dir {
		t.Errorf("expected dataDir=%s, got %s", dir, p.DataDir())
	}
}

func TestPartitionCleanupData(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}

	p.Append(message.NewRecordWithString("", "data"))

	// 清理数据目录
	if err := p.CleanupData(); err != nil {
		t.Fatalf("cleanup data: %v", err)
	}

	// 目录应该被删除
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Error("data directory should be removed after CleanupData")
	}
}

// ─── 跨 segment 边界读取测试 ─────────────────────

func TestPartitionReadCrossSegmentBoundary(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}
	defer p.Close()

	// 设置 segment 只能装 2 条消息左右
	p.SetMaxSegmentSize(100)

	// 写入 10 条
	for i := 0; i < 10; i++ {
		p.Append(message.NewRecordWithString("", string(rune('0'+i))))
	}

	// 跨 segment 读取 offset 0~9
	msgs := p.Read(0, 10)
	if len(msgs) != 10 {
		t.Fatalf("expected 10 messages, got %d", len(msgs))
	}
	for i, m := range msgs {
		if m.Offset != int64(i) {
			t.Errorf("msg[%d]: expected offset=%d, got %d", i, i, m.Offset)
		}
		if string(m.Value) != string(rune('0'+i)) {
			t.Errorf("msg[%d]: expected value=%c, got %s", i, '0'+i, m.Value)
		}
	}
}

func TestPartitionGetCrossSegment(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPartitionWithDir(0, dir)
	if err != nil {
		t.Fatalf("create partition: %v", err)
	}
	defer p.Close()

	p.SetMaxSegmentSize(100)

	for i := 0; i < 10; i++ {
		p.Append(message.NewRecordWithString("", string(rune('A'+i))))
	}

	// 测试每个 offset 都能读到
	for i := 0; i < 10; i++ {
		msg := p.Get(int64(i))
		if msg == nil {
			t.Errorf("offset %d: expected message, got nil", i)
			continue
		}
		if string(msg.Value) != string(rune('A'+i)) {
			t.Errorf("offset %d: expected value=%c, got %s", i, 'A'+i, msg.Value)
		}
	}
}

// ─── Partition 字符串表示测试 ─────────────────────

func TestPartitionString(t *testing.T) {
	p := NewPartition(0)
	p.Append(message.NewRecordWithString("", "hello"))
	s := p.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
	// 应包含关键信息
	if p.ID() != 0 {
		t.Errorf("expected id=0, got %d", p.ID())
	}
}
