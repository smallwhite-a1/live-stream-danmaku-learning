# V1: 单机 WebSocket 房间弹幕最小闭环

这个目录是原项目的第一阶段复现版本。V1 的目标不是性能极限，而是让你先看懂一个弹幕系统最小可运行内核：

```text
客户端 A 发送弹幕
        |
        v
服务端 ReadPump 读取 WebSocket 消息
        |
        v
Manager.Broadcast channel
        |
        v
Manager 找到同一房间的所有客户端
        |
        v
safeSend 投递到每个客户端自己的 Send channel
        |
        v
每个客户端的 WritePump 写回 WebSocket
```

V1 不使用 Redis、Kafka、MySQL、Gin、GORM。你只需要理解 Go、`net/http`、`gorilla/websocket`、goroutine、channel、mutex。

## 目录结构

```text
v1/
├── README.md
├── cmd/
│   ├── server/
│   │   └── main.go          # 启动 WebSocket 服务
│   └── client/
│       └── main.go          # 命令行客户端
└── internal/
    ├── model/
    │   └── message.go       # WebSocket 协议结构
    └── ws/
        ├── handler.go       # HTTP Upgrade -> WebSocket
        ├── client.go        # 单个连接的读写 goroutine
        └── manager.go       # 房间管理与广播
```

## 如何运行

打开第一个终端，启动服务端：

```bash
go run ./v1/cmd/server -port 8080
```

打开第二个终端，启动用户 Alice：

```bash
go run ./v1/cmd/client -uid 1001 -name alice -room room1 -port 8080
```

打开第三个终端，启动用户 Bob：

```bash
go run ./v1/cmd/client -uid 1002 -name bob -room room1 -port 8080
```

在 Alice 或 Bob 的终端里输入任意文字并回车。只要两个人在同一个 `room1`，两边都会收到同一条弹幕。

再开一个不同房间的用户：

```bash
go run ./v1/cmd/client -uid 2001 -name carol -room room2 -port 8080
```

Carol 在 `room2`，不会收到 `room1` 的弹幕。这就是“房间隔离”。

## WebSocket 协议

所有 WebSocket 消息都有一个外层信封 `Packet`：

```json
{
  "type": 101,
  "room_id": "room1",
  "data": {}
}
```

V1 只实现一种消息：

```go
const TypeDanmaku = 101
```

客户端发送时只需要给内容：

```json
{
  "type": 101,
  "data": {
    "content": "hello"
  }
}
```

服务端收到后会补全可信字段：

```json
{
  "type": 101,
  "room_id": "room1",
  "data": {
    "room_id": "room1",
    "user_id": "1001",
    "username": "alice",
    "content": "hello",
    "send_time": "2026-06-26T..."
  }
}
```

为什么不让客户端自己传 `user_id`、`room_id`、`send_time`？因为客户端不可信。真实项目里用户身份通常来自登录态/JWT，时间也应该以服务端收到消息的时间为准。

## 核心文件怎么读

建议按这个顺序读：

1. `v1/cmd/server/main.go`
2. `v1/internal/ws/handler.go`
3. `v1/internal/ws/client.go`
4. `v1/internal/ws/manager.go`
5. `v1/cmd/client/main.go`

### server/main.go

服务端做三件事：

```go
manager := ws.NewManager()
go manager.Run()

http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
    ws.ServeWS(manager, w, r)
})
```

第一行创建房间管理器。第二行用 goroutine 启动管理器的事件循环。第三段注册 `/ws` 路由。

这里第一个重要概念是 goroutine：

```go
go manager.Run()
```

这表示 `manager.Run()` 会在一个新的轻量级线程里执行。主 goroutine 继续往下启动 HTTP server。否则如果直接写 `manager.Run()`，它里面是无限循环，程序就走不到 `http.ListenAndServe`。

### handler.go

`ServeWS` 负责把一次普通 HTTP 请求升级成 WebSocket 长连接：

```go
conn, err := upgrader.Upgrade(w, r, nil)
```

升级成功后，服务端会创建一个 `Client`：

```go
client := NewClient(userID, username, roomID, conn)
manager.Register <- client
```

这行是 channel 发送：

```go
manager.Register <- client
```

意思是：把这个新客户端交给 Manager。Manager 的 `Run()` 循环会从 `Register` channel 里取出它，然后加入房间 map。

接着启动写 goroutine：

```go
go client.WritePump()
client.ReadPump(manager)
```

这里故意只给 `WritePump` 加 `go`，而 `ReadPump` 不加。

原因是：每个 WebSocket 连接由一个 HTTP handler goroutine 负责。这个 goroutine 可以直接用来读客户端消息。写消息则需要另一个 goroutine，否则读和写会互相卡住。

### client.go

`Client` 是服务端眼中的一个用户连接：

```go
type Client struct {
    UserID   string
    Username string
    RoomID   string

    Conn *websocket.Conn
    Send chan []byte

    done chan struct{}
    once sync.Once
}
```

最重要的是 `Send chan []byte`。

你可以把它理解成“这个用户的待发送消息队列”。Manager 不直接写 socket，而是把消息放进 `client.Send`。真正写 socket 的动作由 `WritePump` 完成。

这样做有两个好处：

1. Manager 不需要关心网络写入慢不慢。
2. 同一个 WebSocket 连接只有 `WritePump` 一个 goroutine 写，避免并发写 socket。

`ReadPump` 做的是上行链路：

```text
浏览器/客户端 -> WebSocket -> ReadPump -> Manager.Broadcast
```

流程是：

1. `ReadMessage()` 读一条客户端消息。
2. `json.Unmarshal` 解析外层 `Packet`。
3. 如果 `type == 101`，解析弹幕内容。
4. 服务端补上 `room_id/user_id/username/send_time`。
5. 把补全后的 `Packet` 发送到 `manager.Broadcast`。

`WritePump` 做的是下行链路：

```text
Manager.safeSend -> client.Send -> WritePump -> WebSocket -> 客户端
```

它一直监听两个 channel：

```go
select {
case msg := <-c.Send:
    c.Conn.WriteMessage(websocket.TextMessage, msg)
case <-c.done:
    return
}
```

`select` 可以同时等待多个 channel。哪个 channel 先就绪，就执行哪个 case。

### manager.go

Manager 是 V1 的核心。它保存所有房间：

```go
rooms map[string]map[*Client]struct{}
```

含义是：

```text
room1 -> clientA, clientB, clientC
room2 -> clientD, clientE
```

为什么 map 的 value 是 `struct{}`？

因为我们只关心“这个 client 是否在集合里”，不需要额外值。`struct{}` 是 Go 里零大小类型，常用来表示 set。

Manager 有三个 channel：

```go
Register   chan *Client
Unregister chan *Client
Broadcast  chan *model.Packet
```

你可以把 Manager 想成一个小型调度中心：

```go
for {
    select {
    case client := <-m.Register:
        m.handleRegister(client)
    case client := <-m.Unregister:
        m.handleUnregister(client)
    case packet := <-m.Broadcast:
        m.handleBroadcast(packet)
    }
}
```

这就是典型的 Go 并发风格：不同 goroutine 不直接改共享状态，而是通过 channel 把事件交给一个中心循环处理。

## mutex 在 V1 里做什么

Manager 里有：

```go
mu sync.RWMutex
rooms map[string]map[*Client]struct{}
```

`rooms` 是共享数据结构。只要未来有多个 goroutine 可能读写它，就必须保护。

`Lock()` 用于写：

```go
m.mu.Lock()
defer m.mu.Unlock()
```

注册和注销用户会修改 map，所以用写锁。

`RLock()` 用于读：

```go
m.mu.RLock()
defer m.mu.RUnlock()
```

广播时只需要读取某个房间有哪些客户端，所以用读锁。

严格说，V1 当前主要通过 `Manager.Run()` 串行处理事件，mutex 的必要性没有原项目那么强。这里保留 mutex 是为了让 V1 和原项目的设计更容易衔接。原项目里 Redis 订阅 goroutine、统计广播 goroutine、Manager 主循环都会接触房间数据，mutex 就变成必需品。

## 为什么广播时要先复制客户端列表

V1 广播不是这样写：

```go
m.mu.RLock()
for client := range m.rooms[roomID] {
    m.safeSend(client, payload)
}
m.mu.RUnlock()
```

而是这样：

```go
clients := m.snapshotRoomClients(packet.RoomID)
for _, client := range clients {
    m.safeSend(client, payload)
}
```

`snapshotRoomClients` 会在锁内复制一份客户端 slice，然后马上释放锁。

这样做的好处是：锁只保护 map 读取，不包住后续发送逻辑。真实项目里发送可能很慢，如果一直拿着锁，其他用户加入/离开房间都会被卡住。

这个思想非常重要：

```text
锁保护共享内存，不保护慢操作。
```

## safeSend 为什么重要

普通发送是：

```go
client.Send <- payload
```

如果 `client.Send` 满了，这行代码会阻塞。假设某个客户端网络很慢，它的 `WritePump` 消费不过来，那么整个房间广播都可能卡住。

所以 V1 使用非阻塞发送：

```go
select {
case client.Send <- payload:
default:
    log.Printf("drop message for slow client")
}
```

含义是：

1. 如果 `client.Send` 还有空间，就把消息放进去。
2. 如果满了，立即走 `default`，丢弃这条消息。

弹幕系统通常更看重实时性。慢客户端少看一两条弹幕，比所有人一起卡住更能接受。

## 为什么不关闭 client.Send

`Client.Close()` 里只关闭 `done`，不关闭 `Send`：

```go
close(c.done)
_ = c.Conn.Close()
```

这是为了避免 panic。

广播时可能已经复制了一份客户端列表，里面还有某个刚刚断开的 client。如果此时关闭了 `client.Send`，另一个 goroutine 再执行：

```go
client.Send <- payload
```

Go 会直接 panic：`send on closed channel`。

所以 V1 的策略是：

1. 用 `done` 通知 `WritePump` 退出。
2. 关闭真实 socket。
3. 不关闭 `Send`。
4. 让 Manager 从房间 map 删除这个 client。

这是原项目 `safeSend` 设计的简化版。

## V1 和原项目的关系

V1 已经包含原项目最关键的实时闭环：

```text
ConnectWebSocket
-> Client.ReadPump
-> Manager.Broadcast
-> broadcast room clients
-> Client.WritePump
```

V1 暂时没有：

```text
Redis Pub/Sub        # 多台 server 跨进程广播
Kafka Producer      # 弹幕异步进入消息队列
Kafka Consumer      # 批量消费并落库
MySQL/GORM          # 历史弹幕持久化
点赞/在线统计       # Redis 计数和定时广播
worker pool         # 多 worker 并行扇出
sync.Pool           # 复用 slice 降低 GC
```

等 V1 熟悉后，下一步建议做 V2：

1. 把 `Manager.handleBroadcast` 改成投递 `BroadcastJob`。
2. 启动固定数量 worker goroutine。
3. 由 worker 调用 `safeSend`。

这就是原项目 `broadcastJobChan + WorkerCount` 的雏形。

## 你可以怎么学习和调试

第一步，只跑两个客户端，确认同房间能互相收到。

第二步，在 `Manager.handleRegister`、`handleBroadcast`、`safeSend` 里加日志，看一条消息经过哪里。

第三步，把 `SendBufferSize` 改成 `1`，然后尝试制造慢客户端，观察 `safeSend` 的 drop 日志。

第四步，把 `snapshotRoomClients` 改成直接在锁内发送，再思考为什么这在高并发下不好。

第五步，用下面命令开多个客户端，观察房间隔离：

```bash
go run ./v1/cmd/client -uid 1 -name u1 -room room1
go run ./v1/cmd/client -uid 2 -name u2 -room room1
go run ./v1/cmd/client -uid 3 -name u3 -room room2
```

## 常见问题

### 为什么服务端需要两个 pump？

WebSocket 是一条双向连接。读和写是两个方向：

```text
ReadPump:  客户端 -> 服务端
WritePump: 服务端 -> 客户端
```

如果只用一个循环，很容易出现“正在等客户端发消息，所以没法给客户端写消息”的问题。拆成两个 goroutine 后，读写互不阻塞。

### channel 是队列吗？

可以先这样理解。`client.Send` 是每个用户自己的发送队列，`manager.Broadcast` 是所有上行弹幕进入 Manager 的队列。

但 channel 不只是队列，它还带同步语义。无缓冲 channel 会让发送方和接收方直接会合；有缓冲 channel 可以暂存一些消息。

V1 里：

```go
Broadcast: make(chan *model.Packet, 128)
Send:      make(chan []byte, 32)
```

都使用了缓冲，目的是吸收短时间的小峰值。

### 为什么不用 worker pool？

V1 先不用，是为了让链路更短。你先理解一个 Manager 串行处理广播。理解之后再加 worker pool，会非常自然：

```text
V1:
Manager -> safeSend

V2:
Manager -> broadcastJobChan -> worker goroutines -> safeSend
```

### 这一版能支撑高并发吗？

不能把它当最终高并发版本。V1 是学习版，重点是正确的并发模型。性能增强会在后续版本逐步加入。

### V1 为什么不写数据库？

因为数据库会引入另一个问题：实时广播不应该等数据库写入完成。真实项目用 Kafka 做缓冲，让 server 先广播，再异步落库。这个思路放到 V4/V5 学更合适。
