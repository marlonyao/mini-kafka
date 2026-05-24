# Mini-Kafka 📨

一个极简的 Apache Kafka 实现，用于学习 Kafka 的核心设计思想。

## 构建步骤

| Step | 内容 | 核心知识点 |
|------|------|-----------|
| 1 | Topic + Partition + 消息存储 | 消息追加, 分区有序, offset |
| 2 | Producer + 分区策略 | 轮询, 按键哈希, 指定分区 |
| 3 | Consumer + ConsumerGroup + Offset | 拉取模型, 位移管理, 组内负载均衡 |
| 4 | Rebalance 再均衡 | 消费者变动时分区重分配 |
| 5 | 消息持久化 + 日志分段 | 顺序写, Segment 滚动, 日志清理 |
| 6 | Broker/Client 网络架构 | TCP 协议, 请求/响应, Client/Server 模式 |

## 运行测试

```bash
cd mini-kafka
go test ./pkg/... -v
```

## 学习方法

每一步对应一个构建步骤，先看测试理解行为，再看实现。

## Kafka 核心概念速览

```
Producer → [Topic: orders]
             ├── Partition 0: [msg0, msg1, msg2, ...]
             ├── Partition 1: [msg0, msg1, msg2, ...]
             └── Partition 2: [msg0, msg1, msg2, ...]
                                        ↓
          Consumer Group "analytics"
             ├── Consumer A ← Partition 0
             ├── Consumer B ← Partition 1
             └── Consumer C ← Partition 2
```
