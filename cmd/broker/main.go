// Mini-Kafka Broker 服务端
//
// 启动 Broker，监听 TCP 连接，接受客户端请求。
//
// 用法：
//   go run main.go -addr :9092 -dataDir /tmp/mini-kafka-data
//   go run main.go -addr :9092                # 纯内存模式
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/marlonyao/mini-kafka/pkg/broker"
)

func main() {
	addr := flag.String("addr", ":9092", "监听地址")
	dataDir := flag.String("dataDir", "", "数据持久化目录（空=纯内存模式）")
	flag.Parse()

	b := broker.NewBroker(*addr, *dataDir)

	// 优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\n🛑 正在关闭 Broker...")
		b.Close()
	}()

	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("  📨 Mini-Kafka Broker")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("  监听地址: %s\n", *addr)
	if *dataDir != "" {
		fmt.Printf("  数据目录: %s (持久化模式)\n", *dataDir)
	} else {
		fmt.Printf("  数据模式: 纯内存 (重启数据丢失)\n")
	}
	fmt.Println("  按 Ctrl+C 停止")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println()

	if err := b.Start(); err != nil {
		log.Fatalf("Broker 启动失败: %v", err)
	}
}
