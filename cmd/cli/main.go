// Mini-Kafka 命令行客户端
//
// 用法：
//   BROKER=localhost:9092 go run main.go create-topic orders -p 3
//   BROKER=localhost:9092 go run main.go produce orders "hello world"
//   BROKER=localhost:9092 go run main.go consume orders -p 0 -n 10
//
// 或者：
//   go run main.go create-topic orders -p 3              (默认连接 localhost:9092)
//   go run main.go produce orders "hello" -k user1
//   go run main.go consume orders -p 0 -n 10 -g mygroup
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/marlonyao/mini-kafka/pkg/client"
	"github.com/marlonyao/mini-kafka/pkg/protocol"
)

func getBrokerAddr() string {
	if addr := os.Getenv("BROKER"); addr != "" {
		return addr
	}
	return "localhost:9092"
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "create-topic":
		runCreateTopic()
	case "produce":
		runProduce()
	case "consume":
		runConsume()
	default:
		fmt.Printf("未知命令: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("  📨 Mini-Kafka CLI 客户端")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println()
	fmt.Println("环境变量:")
	fmt.Println("  BROKER  Broker 地址 (默认 localhost:9092)")
	fmt.Println()
	fmt.Println("命令:")
	fmt.Println("  create-topic <name> [-p 3]        创建 Topic")
	fmt.Println("  produce <topic> <message> [-k key] 发送消息")
	fmt.Println("  consume <topic> [-g group] [-p 0] [-n 10] [-f] 拉取消息")
	fmt.Println()
	fmt.Println("示例:")
	fmt.Println("  BROKER=localhost:9092 go run main.go create-topic orders -p 3")
	fmt.Println("  go run main.go produce orders 'hello' -k user1")
	fmt.Println("  go run main.go consume orders -p 0 -n 10")
	fmt.Println("  go run main.go consume orders -g mygroup -f")
}

// ─── create-topic ───────────────────────────────

func runCreateTopic() {
	fs := flag.NewFlagSet("create-topic", flag.ExitOnError)
	partitions := fs.Int("p", 3, "分区数量")
	reordered := reorderArgs(os.Args[2:])
	fs.Parse(reordered)

	if fs.NArg() < 1 {
		fmt.Println("用法: create-topic <name> [-p partitions]")
		os.Exit(1)
	}
	name := fs.Arg(0)

	admin, err := client.NewAdminClient(getBrokerAddr())
	if err != nil {
		log.Fatalf("❌ 连接 Broker 失败: %v", err)
	}
	defer admin.Close()

	if err := admin.CreateTopic(name, *partitions); err != nil {
		log.Fatalf("❌ 创建 Topic 失败: %v", err)
	}

	fmt.Printf("✅ Topic '%s' 创建成功 (%d 个分区)\n", name, *partitions)
}

// ─── produce ────────────────────────────────────

func runProduce() {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	key := fs.String("k", "", "消息 Key")
	partition := fs.Int("p", -1, "目标分区 (-1=自动)")
	reordered := reorderArgs(os.Args[2:])
	fs.Parse(reordered)

	if fs.NArg() < 2 {
		fmt.Println("用法: produce <topic> <message> [-k key] [-p partition]")
		os.Exit(1)
	}
	topic := fs.Arg(0)
	value := fs.Arg(1)

	producer, err := client.NewClientProducer(getBrokerAddr())
	if err != nil {
		log.Fatalf("❌ 连接 Broker 失败: %v", err)
	}
	defer producer.Close()

	part, offset, err := producer.Send(topic, *key, value, *partition)
	if err != nil {
		log.Fatalf("❌ 发送失败: %v", err)
	}

	fmt.Printf("✅ 发送成功 → Topic=%s Partition=%d Offset=%d\n", topic, part, offset)
	if *key != "" {
		fmt.Printf("   Key=%s\n", *key)
	}
	fmt.Printf("   Value=%s\n", value)
}

// ─── consume ────────────────────────────────────

func runConsume() {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	group := fs.String("g", "", "消费者组 ID")
	partition := fs.Int("p", 0, "分区号")
	startOffset := fs.Int64("o", -1, "起始 offset (-1=从已提交 offset 开始)")
	maxCount := fs.Int("n", 10, "最多拉取条数")
	follow := fs.Bool("f", false, "持续消费模式")
	// 把 flags 放到 args 前面重新排列，确保 flag 能被正确解析
	reordered := reorderArgs(os.Args[2:])
	fs.Parse(reordered)

	if fs.NArg() < 1 {
		fmt.Println("用法: consume <topic> [-g group] [-p partition] [-n count] [-f]")
		os.Exit(1)
	}
	topic := fs.Arg(0)

	c, err := client.NewClientConsumer(getBrokerAddr(), *group, topic)
	if err != nil {
		log.Fatalf("❌ 连接 Broker 失败: %v", err)
	}
	defer c.Close()

	if *group != "" {
		assigned, err := c.JoinGroup()
		if err != nil {
			log.Fatalf("❌ 加入消费者组失败: %v", err)
		}
		fmt.Printf("👥 加入消费者组 '%s', 分配分区: %v\n", *group, assigned)
	}


	// 确定要消费的分区列表
	var partitions []int
	if *group != "" && *partition == 0 && *startOffset < 0 {
		// 消费者组模式 + 未指定分区 → 轮询所有分配到的分区
		assigned, _ := c.JoinGroup() // 已加入过，这里获取分配结果
		if len(assigned) > 0 {
			partitions = assigned
		} else {
			partitions = []int{*partition}
		}
	} else {
		partitions = []int{*partition}
	}

	// 每个分区维护独立的 offset
	offsets := make(map[int]int64)
	for _, p := range partitions {
		if *startOffset >= 0 {
			offsets[p] = *startOffset
		} else if *group != "" {
			committed, err := c.FetchOffset(p)
			if err == nil {
				offsets[p] = committed
			} else {
				offsets[p] = 0
			}
		} else {
			offsets[p] = 0
		}
	}

	if *follow {
		if len(partitions) > 1 {
			fmt.Printf("🔄 持续消费 %s 分区%v (Ctrl+C 停止)\n\n", topic, partitions)
		} else {
			fmt.Printf("🔄 持续消费 %s P%d (Ctrl+C 停止)\n\n", topic, partitions[0])
		}
		for {
			totalMsgs := 0
			for _, p := range partitions {
				msgs, err := c.Poll(p, offsets[p], *maxCount)
				if err != nil {
					log.Printf("P%d 拉取失败: %v", p, err)
					continue
				}
				for _, m := range msgs {
					printMsg(&m)
					offsets[p] = m.Offset + 1
					totalMsgs++
				}
				if *group != "" && len(msgs) > 0 {
					c.CommitOffset(p, offsets[p])
				}
			}
			if totalMsgs == 0 {
				time.Sleep(500 * time.Millisecond)
			}
		}
	} else {
		// 一次性拉取所有分区
		var allMsgs []protocol.FetchedMessage
		for _, p := range partitions {
			msgs, err := c.Poll(p, offsets[p], *maxCount)
			if err != nil {
				log.Fatalf("❌ P%d 拉取失败: %v", p, err)
			}
			for _, m := range msgs {
				offsets[p] = m.Offset + 1
			}
			allMsgs = append(allMsgs, msgs...)
		}
		if len(allMsgs) == 0 {
			fmt.Println("📭 没有消息")
			return
		}
		fmt.Printf("📥 拉取到 %d 条消息:\n", len(allMsgs))
		for i := range allMsgs {
			printMsg(&allMsgs[i])
		}
		if *group != "" {
			for _, p := range partitions {
				if offsets[p] > 0 {
					c.CommitOffset(p, offsets[p])
				}
			}
			fmt.Printf("\n💾 已提交所有分区 offset\n")
		}
	}
}

// ─── 辅助 ──────────────────────────────────────

// reorderArgs 把 -flag 参数移到非 flag 参数前面
// Go 的 flag 包遇到第一个非 flag 参数就会停止解析
// 所以 "orders -p 1" 需要重排为 "-p 1 orders"
func reorderArgs(args []string) []string {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-") {
			flags = append(flags, args[i])
			// 如果是 -key value 形式，把 value 也带上
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				// 检查是不是 bool flag (如 -f)
				if !isBoolFlag(args[i]) {
					i++
					flags = append(flags, args[i])
				}
			}
		} else {
			positional = append(positional, args[i])
		}
	}
	return append(flags, positional...)
}

func isBoolFlag(arg string) bool {
	return arg == "-f"
}

func printMsg(m *protocol.FetchedMessage) {
	key := m.Key
	if key == "" {
		key = "-"
	}
	fmt.Printf("   P%d/O%d  %s → %s\n", m.Partition, m.Offset, key, m.Value)
}
