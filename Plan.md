# centrifuge-plus 实现计划

> 版本：v2.0 | 日期：2026-07-27 | 状态：**已完成**
>
> 需求文档：[REQUIREMENTS.md](REQUIREMENTS.md)
>
> 变更：移除 Asynq 依赖，改为"先落盘再推送"模型。详见 [DRAFT.md](DRAFT.md)。

## 阶段总览

| 阶段 | 内容 | 依赖 | 状态 |
|------|------|------|------|
| 1 | 项目基础（清理依赖、重命名） | 无 | ✅ |
| 2 | Lua 脚本 | 1 | ✅ |
| 3 | TopicBroker | 1, 2 | ✅ |
| 4 | DualBroker | 3 | ✅ |
| 5 | 测试 | 3, 4 | ✅ |
| 6 | 示例程序 | 4, 5 | ✅ |

---

## 阶段 1：项目基础

### 1.1 依赖清理

在 `centrifuge-plus/go.mod` 中移除不再需要的依赖：

```
- github.com/hibiken/asynq
- google.golang.org/protobuf
```

保留的依赖：

- `github.com/centrifugal/centrifuge v0.38.0` — Broker 接口、RedisBroker、Publication 等核心类型
- `github.com/redis/rueidis` — Lua 脚本执行和 PUB/SUB
- `go.opentelemetry.io/otel` — 分布式链路追踪

### 1.2 包结构

```text
centrifuge-plus/
├── channel_type.go          # ChannelType 枚举、WithChannelType option（不变）
├── dual_broker.go           # DualBroker 实现（修改：AsynqBroker → TopicBroker）
├── dual_broker_config.go    # DualBrokerConfig（修改：AsynqBrokerConfig → TopicBrokerConfig）
├── topic_broker.go          # TopicBroker 实现（原 asynq_broker.go，大改）
├── topic_broker_config.go   # TopicBrokerConfig（原 asynq_broker_config.go，精简）
├── history_store.go         # HistoryStore 接口（不变）
├── logger.go                # Logger 接口（不变）
├── tracer.go                # 链路追踪工具（不变）
├── lua/                     # Lua 脚本
│   ├── incrby_offset.lua        # 新增：批量预分配 offset
│   └── publish_with_offset.lua  # 新增：使用预分配 offset 发布到 PUB/SUB
└── example/                 # 示例程序
    ├── main.go
    └── setup.go
```

### 1.3 删除的文件

| 文件 | 原因 |
|------|------|
| `asynq_broker.go` | 重命名为 `topic_broker.go`，内容大改 |
| `asynq_broker_config.go` | 重命名为 `topic_broker_config.go`，内容精简 |
| `lua/add_history_stream.lua` | 拆成 `incrby_offset.lua` + `publish_with_offset.lua` |
| `lua/history_stream.lua` | History 改为只读 HistoryStore，不再需要 |
| `task_processor.go` | Asynq 移除，接入方自己写持久化 |
| `task_payload.go` | Asynq 移除，不再需要 TaskPayload |
| `worker.go` | Asynq 移除，不再需要 AsynqWorker |
| `worker_config.go` | 同上 |
| `internal/proto/asynq.pb.go` | Asynq 移除，不再需要 protobuf |
| `internal/proto/asynq.proto` | 同上 |

### 1.4 核心类型定义

**channel_type.go**（不变）：

```go
type ChannelType int

const (
    Live  ChannelType = iota // 非持久化，走 RedisBroker
    Topic                    // 持久化 + 实时推送，走 TopicBroker
)

// WithChannelType 返回一个 SubscribeOption，指定频道类型
func WithChannelType(ct ChannelType) SubscribeOption
```

**history_store.go**（不变）：

```go
type HistoryStore interface {
    Query(channel string, sinceOffset uint64) ([]*centrifuge.Publication, error)
}
```

---

## 阶段 2：Lua 脚本

### 2.1 incrby_offset.lua（批量预分配 offset）

对每个 channel 的 meta key 执行 `HINCRBY`，原子返回 `{offset, epoch}`。

功能：

1. 遍历 KEYS（每个 channel 的 meta key）
2. 获取或创建 epoch
3. `HINCRBY` 递增 offset
4. 返回 `[offset_1, epoch_1, offset_2, epoch_2, ...]`

KEYS: `[meta_key_1, meta_key_2, ...]`

ARGV: `[new_epoch_if_empty_1, new_epoch_if_empty_2, ...]`

> **参考**：centrifuge 的 `broker_history_add_stream.lua` 中的 epoch + HINCRBY 逻辑。

### 2.2 publish_with_offset.lua（使用预分配 offset 发布）

使用预分配的 offset 发布到 PUB/SUB。**无 HINCRBY，无 XADD，无 Asynq**。

功能：

1. 幂等检查（可选，通过 result_key）
2. 校验 epoch 与 meta 一致（防止过期 offset 被使用）
3. `PUBLISH`/`SPUBLISH` 到 PUB/SUB 频道
4. 缓存幂等结果（可选）
5. 返回 `{offset, epoch, fromCache}`

KEYS: `[meta_key, result_key]`

ARGV: `[message_payload, channel, offset, epoch, publish_command, result_key_expire, traceparent]`

**epoch 不匹配处理**：返回 `{"-1", current_epoch, "0"}`，Go 端解析为明确错误。

> **参考**：centrifuge 的 `broker_history_add_stream.lua` 中的 PUBLISH 逻辑。asynq 的 `enqueueCmd` 已不再需要。

---

## 阶段 3：TopicBroker

### 3.1 结构体定义

```go
type TopicBroker struct {
    eventHandler centrifuge.BrokerEventHandler
    config       TopicBrokerConfig
    redisClient  rueidis.Client
    historyStore HistoryStore

    incrbyOffsetScript      *rueidis.Lua
    publishWithOffsetScript *rueidis.Lua

    prefix string
    logger Logger
    tracer trace.Tracer

    // PUB/SUB support via DedicatedClient
    pubSubClient    rueidis.DedicatedClient
    pubSubCancel    func()
    pubSubMu        sync.Mutex
    subscribedChans map[string]bool
}
```

### 3.2 配置

```go
type TopicBrokerConfig struct {
    Prefix         string
    RedisAddr      string
    RedisPassword  string
    RedisDB        int
    HistoryStore   HistoryStore
    HistoryMetaTTL time.Duration
    Logger         Logger
    Tracing        TracingConfig
}
```

### 3.3 接口实现

#### RegisterBrokerEventHandler

保存 `eventHandler` 引用，用于 `HandlePublication` 回调。

#### Subscribe

1. 在 Redis 中订阅对应的 PUB/SUB 频道
2. 收到消息时，构造 `Publication` 并调用 `eventHandler.HandlePublication()`

#### Unsubscribe

取消 Redis PUB/SUB 订阅。

#### BatchIncrby

```go
func (b *TopicBroker) BatchIncrby(ctx context.Context, reqs []ChannelIncrbyRequest) (map[string]centrifuge.StreamPosition, error)
```

1. 构建 KEYS（每个 channel 的 meta key）
2. 构建 ARGV（每个 channel 的新 epoch）
3. 执行 `incrby_offset.lua`
4. 解析结果，返回 `map[channel]StreamPosition`

#### PublishWithOffset

```go
func (b *TopicBroker) PublishWithOffset(ctx context.Context, ch string, data []byte, opts centrifuge.PublishOptions, sp centrifuge.StreamPosition) error
```

1. 构建 KEYS（`[meta_key, result_key]`）
2. 构建 ARGV（`[payload, channel, offset, epoch, publish_cmd, idempotent_ttl, traceparent]`）
3. 执行 `publish_with_offset.lua`
4. 解析结果
   - `epoch == "-1"` → 返回 epoch 不匹配错误
   - `fromCache == "1"` → 幂等命中，跳过
   - 正常 → 返回 nil

#### Publish / PublishWithContext（便捷方法）

```go
func (b *TopicBroker) Publish(ch string, data []byte, opts centrifuge.PublishOptions) (centrifuge.StreamPosition, bool, error)
```

内部调用：

1. `BatchIncrby([]ChannelIncrbyRequest{{Channel: ch}})`
2. `PublishWithOffset(ch, data, opts, position)`
3. 返回 `StreamPosition`

#### History

```go
func (b *TopicBroker) History(ch string, opts centrifuge.HistoryOptions) ([]*centrifuge.Publication, centrifuge.StreamPosition, error)
```

不再从 Redis Stream 读取，直接从 HistoryStore 读取：

1. 解析 `opts.Filter.Since` 获取 `sinceOffset`
2. 调用 `b.historyStore.Query(ch, sinceOffset)`
3. 从 Redis meta key 读取 `StreamPosition`（top_offset + epoch）
4. 返回 `pubs, sp, err`

#### PublishJoin / PublishLeave

通过 Redis PUB/SUB 发布 join/leave 事件（不变）。

#### RemoveHistory

如果 HistoryStore 实现了删除方法，调用它；否则 no-op。

### 3.4 Closer 接口

实现 `Closer` 接口（`broker.go:93`），优雅关闭 Redis 连接和 PUB/SUB client。

> **参考**：`broker.go:93` — `Closer` 接口定义。

---

## 阶段 4：DualBroker

### 4.1 结构体定义

```go
type DualBroker struct {
    liveBroker   *RedisBroker   // centrifuge 内置
    topicBroker  *TopicBroker   // 自定义
    channelTypes sync.Map       // map[string]ChannelType
}
```

### 4.2 配置

```go
type DualBrokerConfig struct {
    Live  RedisBrokerConfig  // Live 模式 Redis 配置
    Topic TopicBrokerConfig  // Topic 模式配置
}
```

### 4.3 路由逻辑

所有 `Broker` 接口方法的实现遵循相同模式：

```go
func (d *DualBroker) Publish(ch string, data []byte, opts PublishOptions) (StreamPosition, bool, error) {
    switch d.getChannelType(ch) {
    case Topic:
        return d.topicBroker.Publish(ch, data, opts)
    default: // Live
        return d.liveBroker.Publish(ch, data, opts)
    }
}
```

### 4.4 扩展方法

DualBroker 暴露 Topic 模式的扩展方法：

```go
func (d *DualBroker) BatchIncrby(ctx context.Context, reqs []ChannelIncrbyRequest) (map[string]centrifuge.StreamPosition, error) {
    return d.topicBroker.BatchIncrby(ctx, reqs)
}

func (d *DualBroker) PublishWithOffset(ctx context.Context, ch string, data []byte, opts centrifuge.PublishOptions, sp centrifuge.StreamPosition) error {
    return d.topicBroker.PublishWithOffset(ctx, ch, data, opts, sp)
}
```

### 4.5 Subscribe 特殊处理

`Subscribe` 方法需要从 `SubscribeOptions` 中提取 `WithChannelType` 设置的值，注册到 `channelTypes` 映射中，然后委托给对应的底层 broker。

需要研究如何将自定义 option 传递到 `Broker.Subscribe` 方法。centrifuge 的 `Broker.Subscribe` 只接收 channel string，不接收 option。可能的方案：

- 方案 A：DualBroker 在上层（Node.Subscribe 的 wrapper）拦截 option，注册类型后再调用 Node.Subscribe
- 方案 B：利用 centrifuge 的 channel namespace 或 meta 机制

**需要在实现时确认具体方案。**

### 4.6 SetBroker 集成

```go
// 使用方式
broker := NewDualBroker(config)
node.SetBroker(broker)
```

> **参考**：`Node.SetBroker`（`node.go:253`）。

---

## 阶段 5：测试

### 5.1 broker_test.go

适配新接口。主要改动：

- 删除 AsynqWorker 相关测试
- 删除 Redis Stream 读取相关测试
- 重命名 `AsynqBroker` → `TopicBroker` 引用
- 新增 `BatchIncrby` 测试
  - 单 channel 预分配
  - 多 channel 批量预分配
  - 连续调用 offset 递增
  - 新 channel 自动创建 epoch
- 新增 `PublishWithOffset` 测试
  - 正常发布
  - epoch 不匹配返回错误
  - 幂等缓存命中
- 修改 `History` 测试
  - 只从 HistoryStore 读取
  - HistoryStore 为空时返回空列表
- 保留 `Publish` 便捷方法测试

### 5.2 dual_broker_test.go

- 路由逻辑测试（Live/Topic 分发）
- `BatchIncrby`/`PublishWithOffset` 路由测试

---

## 阶段 6：示例程序

### 6.1 结构

```
example/
├── main.go           # 入口：启动 server + 4 个 client
├── server.go         # HTTP server + centrifuge Node + DualBroker
├── client.go         # 设备客户端逻辑
├── history_store.go  # HistoryStore 实现（从 SQLite 读取）
└── setup.go          # 数据清理、测试数据准备
```

### 6.2 Server 启动流程

```
1. 清理 Redis 和 SQLite 残留数据
2. 初始化 SQLite
3. 创建 DualBroker（配置 Live Redis + Topic Broker + HistoryStore）
4. 创建 centrifuge Node，SetBroker(dualBroker)
5. 注册 centrifuge 事件回调（OnPublish 等）
6. 启动 HTTP server（WebSocket endpoint）
```

注意：不再启动 AsynqWorker。持久化逻辑在 OnPublish 回调中通过 DB 事务完成。

### 6.3 Client 流程

每个设备（goroutine）：

```
1. 连接 WebSocket
2. 订阅所有频道（指定 Live 或 Topic 类型）
3. 测试 1：发送持久化消息到 group-1, channel-1, channel-2
   - 验证所有在线成员实时收到
4. 测试 2：模拟部分成员离线
   - 发送持久化消息
   - 离线成员重新连接，通过主动拉取 DB 验证恢复消息
5. 测试 3：发送 streaming text 到三个对话
   - 逐词发送，走 Live 频道
   - 验证打字机效果实时可达
```

### 6.4 写扩散 / 读扩散演示

- 写扩散：group-1 的消息由 server 端复制到每个成员的个人 topic 频道（BatchIncrby 多 channel）
- 读扩散：channel-1/2 的消息直接发到公共 topic 频道（BatchIncrby 单 channel）

---

## 关键设计决策摘要

| 决策 | 选择 | 原因 |
|------|------|------|
| 频道路由 | 参数（WithChannelType） | 不侵入频道命名空间，避免与 centrifuge 的 `$` 前缀冲突 |
| Topic 推送模型 | 先落盘再推送（BatchIncrby → DB 事务 → PublishWithOffset） | 事务回滚不留脏数据 |
| 持久化方式 | 接入方 DB 事务内同步写入 | 移除 Asynq，简化架构 |
| 实时推送 | 纯 PUBLISH（无 XADD stream） | History 统一从 DB 读取，Stream 只写不读是冗余 |
| Recovery 策略 | 客户端主动拉取 DB | 不依赖 Redis Stream，数据源单一 |
| History 读取 | 只读 HistoryStore | 数据源单一，无合并逻辑 |
| Lua 脚本 | 2 个（incrby + publish） | 职责清晰：预分配 offset、发布消息 |
| 幂等机制 | 保留 result_key | IM 场景网络重试常见，防止重复消息 |
| 便捷 Publish | 保留 | 非 IM 场景（简单通知）可直接使用 |
| 命名 | TopicBroker（原 AsynqBroker） | Asynq 移除，名称应反映实际用途 |
| Asynq 依赖 | 完全移除 | 持久化由接入方 DB 事务完成，不再需要任务队列 |
