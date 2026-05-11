// Mini-Kafka 实战示例：电商订单处理系统
//
// 演示 Mini-Kafka 的完整使用流程：
// 1. 创建 Topic（订单主题，3个分区）
// 2. Producer 发送订单（按键哈希分区）
// 3. Consumer Group 并行消费
// 4. Offset 提交和恢复
// 5. 消费者 Rebalance
package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	mini "github.com/marlonyao/mini-kafka/pkg"
	"github.com/marlonyao/mini-kafka/pkg/message"
)

func main() {
	fmt.Println("\n" + strings.Repeat("📨", 25))
	fmt.Println("   Mini-Kafka 实战示例：电商订单处理系统")
	fmt.Println(strings.Repeat("📨", 25))

	// ═══════════════════════════════════════════════
	// 示例1：基础 - 创建 Topic + 发送 + 消费
	// ═══════════════════════════════════════════════
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📦 示例1：创建订单 Topic + 发送 + 消费")
	fmt.Println(strings.Repeat("=", 60))

	// 创建 Topic：3 个分区（可以 3 个消费者并行）
	topic := mini.NewTopic("orders", 3)
	fmt.Printf("\n创建 Topic: %s\n\n", topic)

	// 创建 Producer（使用哈希分区策略）
	producer := mini.NewProducer(topic, mini.NewHashPartitioner())

	// 发送订单：用 user_id 作为 key → 同一用户的订单去同一分区
	orders := []struct {
		userID string
		item   string
		amount float64
	}{
		{"user_1", "iPhone 15", 7999},
		{"user_2", "MacBook Pro", 14999},
		{"user_1", "AirPods Pro", 1899},
		{"user_3", "iPad Air", 4799},
		{"user_2", "Apple Watch", 2999},
		{"user_1", "Magic Keyboard", 2499},
		{"user_3", "HomePod", 2299},
		{"user_4", "AirTag", 299},
		{"user_2", "iMac", 10999},
	}

	fmt.Println("📤 发送订单（按 user_id 哈希分区）：")
	for _, order := range orders {
		value := fmt.Sprintf("%s|%s|%.0f", order.userID, order.item, order.amount)
		msg, err := producer.Send(message.NewRecordWithString(order.userID, value))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   %s → 分区%d, offset=%d\n",
			order.item, msg.Partition, msg.Offset)
	}

	fmt.Printf("\n📊 Topic 状态: %s\n", topic)
	for _, p := range topic.Partitions() {
		fmt.Printf("   Partition %d: %d 条消息, latestOffset=%d\n",
			p.ID(), p.Size(), p.LatestOffset())
	}

	// ═══════════════════════════════════════════════
	// 示例2：Consumer Group 并行消费
	// ═══════════════════════════════════════════════
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("👥 示例2：Consumer Group 并行消费")
	fmt.Println(strings.Repeat("=", 60))

	offsets := mini.NewConsumerOffset()
	cg := mini.NewConsumerGroup("order-processors", topic, offsets)

	c1 := mini.NewConsumer("processor-A", "order-processors", topic, offsets)
	c2 := mini.NewConsumer("processor-B", "order-processors", topic, offsets)
	cg.Join(c1)
	cg.Join(c2)

	fmt.Println("\n消费者分配结果：")
	for _, c := range []*mini.Consumer{c1, c2} {
		fmt.Printf("   %s → 分区 %v\n", c.ID(), c.AssignedPartitions())
	}

	// 并行消费
	fmt.Println("\n📥 并行消费结果：")
	results := cg.PollAll(100)
	for cid, msgs := range results {
		fmt.Printf("\n   %s 消费了 %d 条消息：\n", cid, len(msgs))
		for _, msg := range msgs {
			parts := strings.Split(string(msg.Value), "|")
			fmt.Printf("      [P%d/O%d] %s 买了 %s (¥%s)\n",
				msg.Partition, msg.Offset,
				parts[0], parts[1], parts[2])
		}
	}

	// ═══════════════════════════════════════════════
	// 示例3：Offset 提交 + 故障恢复
	// ═══════════════════════════════════════════════
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("💾 示例3：Offset 提交 + 恢复")
	fmt.Println(strings.Repeat("=", 60))

	// 提交当前 offset
	cg.CommitAll()
	fmt.Println("\n✅ 已提交所有 offset")

	for _, c := range []*mini.Consumer{c1, c2} {
		for _, pid := range c.AssignedPartitions() {
			committed := offsets.Get("order-processors", "orders", pid)
			fmt.Printf("   分区%d: committed offset=%d\n", pid, committed)
		}
	}

	// 模拟新增消息
	fmt.Println("\n📤 新增 3 条订单：")
	for i := 0; i < 3; i++ {
		userID := fmt.Sprintf("user_%d", i+5)
		value := fmt.Sprintf("%s|new_item_%d|%d", userID, i, (i+1)*100)
		producer.Send(message.NewRecordWithString(userID, value))
	}

	// 新消费者从已提交的 offset 恢复（只消费新增的消息）
	fmt.Println("\n🔄 新消费者从已提交 offset 恢复：")
	offsets2 := offsets // 共享 offset 存储
	cg2 := mini.NewConsumerGroup("order-processors", topic, offsets2)
	c3 := mini.NewConsumer("processor-C", "order-processors", topic, offsets2)
	cg2.Join(c3)

	results2 := cg2.PollAll(100)
	totalNew := 0
	for _, msgs := range results2 {
		totalNew += len(msgs)
	}
	fmt.Printf("   恢复后消费到 %d 条新消息（跳过了已处理的旧消息）\n", totalNew)

	// ═══════════════════════════════════════════════
	// 示例4：Rebalance — 消费者变动
	// ═══════════════════════════════════════════════
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🔄 示例4：Rebalance — 消费者变动")
	fmt.Println(strings.Repeat("=", 60))

	cg3 := mini.NewConsumerGroup("analytics", topic, mini.NewConsumerOffset())
	ca := mini.NewConsumer("analytics-A", "analytics", topic, mini.NewConsumerOffset())
	cb := mini.NewConsumer("analytics-B", "analytics", topic, mini.NewConsumerOffset())

	fmt.Println("\n初始：2个消费者")
	cg3.Join(ca)
	cg3.Join(cb)
	fmt.Printf("   %s → 分区 %v\n", ca.ID(), ca.AssignedPartitions())
	fmt.Printf("   %s → 分区 %v\n", cb.ID(), cb.AssignedPartitions())

	// 消费者 C 加入 → 触发 Rebalance
	cc := mini.NewConsumer("analytics-C", "analytics", topic, mini.NewConsumerOffset())
	fmt.Println("\nanalytics-C 加入 → Rebalance！")
	cg3.Join(cc)
	fmt.Printf("   %s → 分区 %v\n", ca.ID(), ca.AssignedPartitions())
	fmt.Printf("   %s → 分区 %v\n", cb.ID(), cb.AssignedPartitions())
	fmt.Printf("   %s → 分区 %v\n", cc.ID(), cc.AssignedPartitions())

	// 消费者 B 离开 → 再次 Rebalance
	fmt.Println("\nanalytics-B 离开 → 再次 Rebalance！")
	cg3.Leave("analytics-B")
	fmt.Printf("   %s → 分区 %v\n", ca.ID(), ca.AssignedPartitions())
	fmt.Printf("   %s → 分区 %v\n", cc.ID(), cc.AssignedPartitions())

	// ═══════════════════════════════════════════════
	// 示例5：不同分区策略对比
	// ═══════════════════════════════════════════════
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🎯 示例5：分区策略对比")
	fmt.Println(strings.Repeat("=", 60))

	// 轮询策略
	t1 := mini.NewTopic("round-robin", 3)
	p1 := mini.NewProducer(t1, mini.NewRoundRobinPartitioner())
	for i := 0; i < 6; i++ {
		p1.Send(message.NewRecordWithString("", fmt.Sprintf("msg%d", i)))
	}
	fmt.Println("\n轮询策略（均匀分布）：")
	for _, p := range t1.Partitions() {
		fmt.Printf("   P%d: %d 条消息\n", p.ID(), p.Size())
	}

	// 哈希策略
	t2 := mini.NewTopic("hash", 3)
	p2 := mini.NewProducer(t2, mini.NewHashPartitioner())
	keys := []string{"user_A", "user_B", "user_A", "user_C", "user_B", "user_A"}
	for _, key := range keys {
		p2.Send(message.NewRecordWithString(key, "event"))
	}
	fmt.Println("\n哈希策略（同 key 同分区）：")
	for _, p := range t2.Partitions() {
		if p.Size() > 0 {
			msgs := p.ReadAll()
			keys := make([]string, 0)
			for _, m := range msgs {
				keys = append(keys, m.Key)
			}
			fmt.Printf("   P%d: %d 条 → keys=%v\n", p.ID(), p.Size(), keys)
		}
	}

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("✅ 所有示例运行完毕！")
	fmt.Println(strings.Repeat("=", 60))

	_ = time.Now() // avoid unused import
}
