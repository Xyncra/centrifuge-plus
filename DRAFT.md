# centrifuge-plus 改造草案：先落盘再推送

> 日期：2026-07-27 | 状态：**已实施**
>
> 背景：IM 场景下，当前设计（Publish 原子完成 stream + PUB/SUB + Asynq 入队）无法满足"事务回滚不留脏数据"的要求。改造为：先通过 BatchIncrby 预分配 offset → DB 事务内写入持久化数据 → 事务提交后再走 broker 推送。
>
> **实施结果**：所有改动已完成，34 个单元测试全部通过。详见 [Requirements.md](Requirements.md)（v2.0）和 [Plan.md](Plan.md)（v2.0）。

## 1. 核心设计变化

```
当前: Publish → Lua(原子: HINCRBY + XADD + PUBLISH + Asynq入队) → AsynqWorker → 写DB
改造: BatchIncrby → DB事务写入 → PublishWithOffset → Lua(PUBLISH)
```

**关键原则**：数据先落盘，再推送。Asynq 不再需要（接入方自己在事务内写入持久化数据）。

**数据源变化**：

| 维度 | 当前 | 改造后 |
|------|------|--------|
| 持久化 | AsynqWorker 异步写入 | 接入方 DB 事务内同步写入 |
| 实时推送 | XADD stream + PUBLISH | 纯 PUBLISH |
| Recovery 热数据 | Redis Stream | 无（客户端必须主动拉取） |
| Recovery 冷数据 | HistoryStore（DB） | HistoryStore（DB），唯一数据源 |

**Stream 去除的原因**：既然 History 只读 HistoryStore（DB），Redis Stream 变成了只写不读的冗余层。去除后 Lua 脚本更简单，少一次 Redis 写入。客户端的恢复机制统一为：**实时推送（PUB/SUB）+ 主动拉取（DB 轮询）**，不依赖 Stream。

## 2. IM 服务端使用流程

```go
// Step 1: 预分配 offset（Lua 脚本，原子批量 HINCRBY）
positions, _ := broker.BatchIncrby(ctx, []centrifugeplus.ChannelIncrbyRequest{
    {Channel: "u=user1"},
    {Channel: "u=user2"},
})
// positions["u=user1"] = {Offset: 7, Epoch: "xxx"}
// positions["u=user2"] = {Offset: 3, Epoch: "yyy"}

// Step 2: DB 事务（接入方自行实现）
db.Begin()
db.Create(&conversation)
db.Create(&message)
db.Create(&UserUpdate{UserID: "user1", Offset: positions["u=user1"].Offset, ...})
db.Create(&UserUpdate{UserID: "user2", Offset: positions["u=user2"].Offset, ...})
db.Commit()

// Step 3: 事务后推送（尽力而为，失败不影响数据一致性）
broker.PublishWithOffset(ctx, "u=user1", data, opts, positions["u=user1"])
broker.PublishWithOffset(ctx, "u=user2", data, opts, positions["u=user2"])
```

## 3. 边缘场景分析

### Edge Case 1: BatchIncrby 成功，DB 事务回滚

offset 被消耗，`user_update` 表中有 gap（例如 user1 有 offset 5, 6, 8，缺 7）。

**处理方式**：查询 `user_update` 时发现 gap，返回 gap 类型记录：

```json
{"update_type": "gap", "offset": 7, "data": null}
```

客户端收到 gap 后跳过该 offset，继续拉取后续记录。

**关键**：`lua/incrby_offset.lua` 和接入者的查询逻辑需要配合处理 gap。

### Edge Case 2: DB 事务成功，PublishWithOffset 失败

数据已在 DB（conversation + message + user_update），但在线客户端没收到实时推送。

**客户端发现时机**：

| 触发时机 | 能否发现 | 说明 |
|---|---|---|
| broker stream recovery（重连） | ❌ 不能 | 没有 Redis Stream |
| 客户端主动拉取 user_update | ✅ 能 | `SELECT * FROM user_update WHERE user_id=? AND offset > lastOffset` |
| 定期轮询（安全网） | ✅ 能 | 客户端每 N 秒拉一次增量更新 |

**结论**：客户端必须有主动拉取 `user_update` 的机制，不能只依赖 broker 的实时推送。

### Edge Case 3: Epoch 变化（offset 归 1）

当 `top_offset` 归 1 时（新 epoch），旧的 offset 失效。客户端需要检测 epoch 变化并做全量同步。

### Edge Case 4: PublishWithOffset epoch 不匹配

`PublishWithOffset` 的 Lua 脚本会校验 epoch 与 Redis meta 一致。如果 `BatchIncrby` 后 DB 事务耗时很长，期间 epoch 发生了变化（极少见），Lua 返回 `-1`。

**Go 端处理**：返回明确错误，接入方可选择重试（重新 BatchIncrby）或记录日志。

## 4. 改造文件清单

### 4.1 删除

| 文件 | 原因 |
|---|---|
| `lua/add_history_stream.lua` | 拆成两个新脚本，旧的删除 |
| `lua/history_stream.lua` | History 改为只从 HistoryStore 读取，不再读 Redis Stream |
| `task_processor.go` | 不再需要 AsynqWorker，接入方自己写持久化 |
| `worker.go` | 同上 |
| `worker_config.go` | 同上 |
| `task_payload.go` | 不再有 Asynq 任务，不需要 TaskPayload |
| `internal/proto/asynq.pb.go` | 不再需要 asynq TaskMessage protobuf 编码 |
| `internal/proto/asynq.proto` | 同上 |

### 4.2 新增

#### `lua/incrby_offset.lua`

批量预分配 offset。对每个 channel 执行 `HINCRBY meta_key 1`，返回 `{offset, epoch}`。

```lua
-- incrby_offset.lua
-- 批量预分配 offset，原子执行多个 HINCRBY。
--
-- KEYS: [meta_key_1, meta_key_2, ...]
--   每个 channel 的 meta key（{prefix}:meta:{channel}）
--
-- ARGV: [new_epoch_if_empty_1, new_epoch_if_empty_2, ...]
--   每个 channel 的新 epoch（如果 meta 不存在）
--
-- 返回: [offset_1, epoch_1, offset_2, epoch_2, ...]

local results = {}
for i = 1, #KEYS do
    local meta_key = KEYS[i]
    local new_epoch = ARGV[i]

    -- 获取或创建 epoch
    local current_epoch = redis.call("hget", meta_key, "e")
    if current_epoch == false then
        current_epoch = new_epoch
        redis.call("hset", meta_key, "e", current_epoch)
    end

    -- 递增 offset
    local offset = redis.call("hincrby", meta_key, "s", 1)

    table.insert(results, tostring(offset))
    table.insert(results, current_epoch)
end

return results
```

#### `lua/publish_with_offset.lua`

使用预分配的 offset 发布到 PUB/SUB。**无 HINCRBY，无 XADD，无 Asynq**。

```lua
-- publish_with_offset.lua
-- 使用预分配的 offset 发布到 PUB/SUB。
-- offset 由接入方通过 BatchIncrby 预分配，此脚本不递增 offset。
-- 不写入 Redis Stream（History 统一从 HistoryStore 读取）。
--
-- KEYS: [meta_key, result_key]
--   meta_key    -> {prefix}:meta:{channel}
--   result_key  -> {prefix}:result:{channel}（幂等缓存，空字符串表示不启用）
--
-- ARGV: [message_payload, channel, offset, epoch,
--        publish_command, result_key_expire, traceparent]
--   message_payload    -> 要发布的数据
--   channel            -> PUB/SUB 频道名
--   offset             -> 预分配的 offset
--   epoch              -> 预分配的 epoch
--   publish_command    -> "publish" 或 "spublish"
--   result_key_expire  -> 幂等缓存 TTL（空字符串表示不启用）
--   traceparent        -> W3C traceparent（空字符串表示不启用）

local meta_key = KEYS[1]
local result_key = KEYS[2]

local message_payload = ARGV[1]
local channel = ARGV[2]
local offset = tonumber(ARGV[3])
local epoch = ARGV[4]
local publish_command = ARGV[5]
local result_key_expire = ARGV[6]
local traceparent = ARGV[7]

-- 幂等检查
if result_key ~= '' and result_key_expire ~= '' then
    local cached_result = redis.call("hmget", result_key, "e", "s")
    local result_epoch, result_offset = cached_result[1], cached_result[2]
    if result_epoch ~= false then
        return { result_offset, result_epoch, "1" }
    end
end

-- 确认 epoch 与 meta 一致（防止过期的预分配 offset 被使用）
local current_epoch = redis.call("hget", meta_key, "e")
if current_epoch ~= epoch then
    return { "-1", current_epoch, "0" }  -- epoch 不匹配，拒绝发布
end

-- 发布到 PUB/SUB
if channel ~= '' then
    local payload = "__" .. "p1:" .. offset .. ":" .. epoch .. "__" .. message_payload
    if traceparent ~= '' then
        payload = payload .. "__tp:" .. traceparent
    end
    redis.call(publish_command, channel, payload)
end

-- 缓存幂等结果
if result_key ~= '' and result_key_expire ~= '' then
    redis.call("hset", result_key, "e", epoch, "s", offset)
    redis.call("expire", result_key, result_key_expire)
end

return { tostring(offset), epoch, "0" }
```

### 4.3 修改

#### `asynq_broker.go` → 重命名为 `topic_broker.go`

**删除**：

- Asynq 相关常量（`AsynqTaskType`、`asynqDefaultRetry`、`asynqDefaultTimeout`）
- `addHistoryScript` 字段
- `historyStreamScript` 字段
- `PublishWithContext` 方法中的 Asynq 入队逻辑（protobuf 编码、task key 构建等）
- `GetTaskOffset` 方法
- `AckTaskOffset` 方法
- `taskOffsetKey` 方法
- asynq protobuf 编码相关 import
- uuid 相关 import（不再需要生成 task ID）

**重命名结构体**：`AsynqBroker` → `TopicBroker`

**重命名构造函数**：`NewAsynqBroker` → `NewTopicBroker`

**新增 Lua 脚本字段**：

```go
type TopicBroker struct {
    eventHandler        centrifuge.BrokerEventHandler
    config              TopicBrokerConfig
    redisClient         rueidis.Client
    historyStore        HistoryStore

    incrbyOffsetScript      *rueidis.Lua  // 新增
    publishWithOffsetScript *rueidis.Lua  // 新增

    prefix string
    logger Logger
    tracer trace.Tracer

    // PUB/SUB support via DedicatedClient（保持不变）
    pubSubClient    rueidis.DedicatedClient
    pubSubCancel    func()
    pubSubMu        sync.Mutex
    subscribedChans map[string]bool
}
```

**新增方法**：

```go
// BatchIncrby 批量预分配 offset。
// 在 DB 事务前调用，原子获取每个 channel 的下一个 offset。
// 返回 map[channel]StreamPosition。
func (b *TopicBroker) BatchIncrby(ctx context.Context, reqs []ChannelIncrbyRequest) (map[string]centrifuge.StreamPosition, error)

// PublishWithOffset 使用预分配的 offset 发布消息。
// 在 DB 事务提交后调用。Lua 脚本内无 HINCRBY，无 XADD，无 Asynq。
// 返回 error。epoch 不匹配时返回明确错误。
func (b *TopicBroker) PublishWithOffset(ctx context.Context, ch string, data []byte, opts centrifuge.PublishOptions, sp centrifuge.StreamPosition) error
```

**修改 `History` 方法**：

```go
func (b *TopicBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
    // 不再从 Redis Stream 读取
    // 直接从接入者的 HistoryStore 读取
    pubs, err := b.historyStore.Query(ch, sinceOffset)
    // top_offset 和 epoch 从 Redis meta key 读取
    sp := b.getStreamPosition(ch)
    return pubs, sp, err
}
```

**保留 `Publish`/`PublishWithContext` 作为便捷方法**（兼容非 IM 场景）：

```go
// Publish 是便捷方法，内部调用 BatchIncrby + PublishWithOffset。
// 适用于不需要 DB 事务的场景（如简单的实时通知）。
// IM 场景请使用 BatchIncrby + DB事务 + PublishWithOffset。
func (b *TopicBroker) Publish(ch string, data []byte, opts centrifuge.PublishOptions) (centrifuge.StreamPosition, bool, error) {
    // 1. BatchIncrby([]ChannelIncrbyRequest{{Channel: ch}})
    // 2. PublishWithOffset(ch, data, opts, position)
    // 返回 StreamPosition
}
```

**保留**：

- `Subscribe`/`Unsubscribe`（PUB/SUB 管理）
- `PublishJoin`/`PublishLeave`
- `RemoveHistory`
- `Close`
- `getStreamPosition`（从 Redis meta 读取 top_offset 和 epoch）
- `historyFromStream` — **删除**（不再需要从 Stream 读取）
- `mergePublications` — **删除**（不再需要合并双源数据）

#### `topic_broker_config.go`（原 `asynq_broker_config.go`）

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
    // 以下字段已删除：
    // QueueName    — asynq 移除，不再需要
    // BeforeEnqueue — asynq 移除，不再需要
}
```

#### `dual_broker.go`

重命名引用：

```go
type DualBroker struct {
    liveBroker  *RedisBroker   // centrifuge 内置（不变）
    topicBroker *TopicBroker   // 原 *AsynqBroker
    channelTypes sync.Map
}
```

新增路由方法：

```go
func (d *DualBroker) BatchIncrby(ctx context.Context, reqs []ChannelIncrbyRequest) (map[string]centrifuge.StreamPosition, error) {
    return d.topicBroker.BatchIncrby(ctx, reqs)
}

func (d *DualBroker) PublishWithOffset(ctx context.Context, ch string, data []byte, opts centrifuge.PublishOptions, sp centrifuge.StreamPosition) error {
    return d.topicBroker.PublishWithOffset(ctx, ch, data, opts, sp)
}
```

#### `dual_broker_config.go`

```go
type DualBrokerConfig struct {
    Live  RedisBrokerConfig    // Live 模式 Redis 配置（不变）
    Topic TopicBrokerConfig    // 原 AsynqBrokerConfig
}
```

#### `history_store.go`

接口不变。`StreamPosition` 始终从 Redis meta key 读取，不依赖 HistoryStore：

```go
type HistoryStore interface {
    Query(channel string, sinceOffset uint64) ([]*centrifuge.Publication, error)
}
```

#### `broker_test.go`

适配新接口。主要改动：

- 删除 AsynqWorker 相关测试
- 删除 Redis Stream 读取相关测试
- 新增 `BatchIncrby` 测试
- 新增 `PublishWithOffset` 测试
- 修改 `History` 测试（只从 HistoryStore 读取）
- 重命名 `AsynqBroker` → `TopicBroker`

### 4.4 `go.mod` 变更

移除不再需要的依赖：

```
- github.com/hibiken/asynq
- google.golang.org/protobuf
```

`github.com/redis/rueidis` 保留（用于 Lua 脚本执行和 PUB/SUB）。

## 5. 与当前 Requirements.md / Plan.md 的差异

| 维度 | 当前设计 | 改造后 |
|---|---|---|
| offset 来源 | Lua 脚本内 HINCRBY | BatchIncrby 预分配 |
| 持久化时机 | AsynqWorker 异步写入 | DB 事务内同步写入 |
| Asynq 依赖 | 核心组件 | 移除 |
| 实时推送 | XADD stream + PUBLISH | 纯 PUBLISH |
| History 数据源 | Redis Stream + HistoryStore 双源 | 只读 HistoryStore |
| Lua 脚本数量 | 2 个（add + history） | 2 个（incrby + publish） |
| 事务安全性 | Publish 在事务内则回滚留脏数据 | Publish 在事务外，回滚无副作用 |
| TaskPayload | 包含 channel + data + extra | 删除 |
| BeforeEnqueue hook | 支持 | 移除 |
| protobuf 依赖 | 需要（asynq TaskMessage） | 移除 |
| Recovery 策略 | Stream 热数据 + HistoryStore 冷数据 | 客户端主动拉取 DB |
| 命名 | AsynqBroker | TopicBroker |

## 6. 待确认问题

~~1. **幂等机制**：`publish_with_offset.lua` 中是否保留幂等缓存（`result_key`）？~~
→ **保留**。IM 场景下网络重试很常见，没有幂等会导致重复消息。

~~2. **HistoryStore 接口**：是否需要扩展返回 epoch？还是始终从 Redis meta 读取？~~
→ **不改**。`StreamPosition` 始终从 Redis meta key 读取，HistoryStore 接口保持不变。

~~3. **向后兼容**：是否保留 `Publish`/`PublishWithContext`（内部自动 BatchIncrby + PublishWithOffset）作为便捷方法？~~
→ **保留**。方便非 IM 场景（如简单实时通知）使用。IM 场景应使用 BatchIncrby + DB 事务 + PublishWithOffset。

~~4. **asynq 完全移除**：是否还有其他场景需要 asynq？还是彻底移除？~~
→ **彻底移除**。

~~5. **QueueName 配置**：移除 asynq 后，`AsynqBrokerConfig.QueueName` 是否保留？~~
→ **删除**。`BeforeEnqueue` 也一并删除。
