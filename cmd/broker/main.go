// Mini-Kafka Broker 服务端
//
// 启动 Broker，监听 TCP 连接，接受客户端请求。
//
// 用法：
//   go run main.go [-addr :9092]
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
	flag.Parse()

	b := broker.NewBroker(*addr, "")

	// 优雅关闭：捕获 Ctrl+C 信号
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
	fmt.Println("  按 Ctrl+C 停止")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println()

	if err := b.Start(); err != nil {
		log.Fatalf("Broker 启动失败: %v", err)
	}
}
