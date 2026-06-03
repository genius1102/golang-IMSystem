# Go IMSystem — 即时通讯系统

一个使用 Go 语言构建的**多用户即时通讯系统**，支持 TCP 与 WebSocket 双协议，具备群聊、私聊、房间聊天、离线消息持久化、心跳保活与断线重连等完整功能。

---

## 目录

- [功能特性](#功能特性)
- [项目结构](#项目结构)
- [架构设计](#架构设计)
- [快速开始](#快速开始)
- [客户端使用](#客户端使用)
- [通信协议](#通信协议)
- [数据库设计](#数据库设计)
- [WebSocket 接入](#websocket-接入)
- [开发指南](#开发指南)
- [依赖说明](#依赖说明)

---

## 功能特性

| 模块 | 说明 |
|------|------|
| **群聊 (Group Chat)** | 消息广播至所有在线用户 |
| **私聊 (Private Chat)** | 点对点消息路由，支持离线暂存 |
| **房间聊天 (Room Chat)** | 创建/加入/离开/列出房间，房间内独立广播 |
| **用户管理** | 自定义用户名（重复检测），在线用户列表查询 |
| **心跳保活 (PING/PONG)** | 客户端每 30s 发送心跳，90s 超时判定断线 |
| **断线重连** | 指数退避重连，最多 10 次，最长间隔 30s |
| **空闲超时** | 服务端 100s 无消息自动踢出 |
| **离线消息持久化** | 私聊消息落库 SQLite，上线自动推送 |
| **ACK 确认机制** | 消息送达后回执确认，标记已投递 |
| **双协议支持** | TCP（端口 8888）+ WebSocket（端口 8889） |
| **优雅关闭** | 捕获 SIGINT/SIGTERM，广播下线通知，关闭数据库连接 |
| **纯 Go SQLite** | `modernc.org/sqlite`，无需 CGo，便于交叉编译 |

---

## 项目结构

```
golang-IMSystem/
├── cmd/
│   ├── server/
│   │   └── main.go              # 服务端入口
│   ├── client/
│   │   └── main.go              # CLI 客户端入口
│   └── test_integration/
│       └── main.go              # 集成测试（自动化交互测试）
├── internal/
│   ├── protocol/
│   │   ├── message.proto        # Protobuf 消息定义
│   │   ├── message.pb.go        # Proto 生成的 Go 代码
│   │   ├── message.go           # 编解码封装（长度前缀帧格式）
│   │   └── message_ext.go       # 扩展消息类型（心跳、房间、ACK 等）
│   └── server/
│       ├── server.go            # 核心 Server 结构体：监听、广播、调度
│       ├── user.go              # User 结构体：连接管理、消息收发、超时检测
│       ├── room.go              # ChatRoom：房间创建/加入/离开/列表/广播
│       ├── ws.go                # WebSocket 适配器（gorilla/websocket → net.Conn）
│       └── db.go                # SQLite 数据库层：离线消息存取、ACK 标记
├── go.mod
├── go.sum
├── .gitignore
└── README.md
```

---

## 架构设计

```
   Browser (WebSocket)             CLI Client (TCP)
          │                              │
          │ ws://host:8889/ws            │ tcp://host:8888
          ▼                              ▼
  ┌──────────────────┐     ┌─────────────────────┐
  │   ws.go 适配器    │     │   net.Conn (直连)    │
  │ gorilla/ws →      │     │                     │
  │ net.Conn 接口      │     │                     │
  └────────┬─────────┘     └──────────┬──────────┘
           │                          │
           └──────────┬───────────────┘
                      │
           ┌──────────▼──────────┐
           │   Server (核心调度)   │
           │                     │
           │  • 用户注册/注销      │
           │  • 消息类型分发       │
           │  • 广播通道扇出       │
           │  • 房间管理          │
           └──────────┬──────────┘
                      │
           ┌──────────▼──────────┐
           │   SQLite 持久层      │
           │   (imserver.db)     │
           │                     │
           │  • 离线私聊消息       │
           │  • 投递状态标记       │
           └─────────────────────┘
```

### 并发模型

- 每个用户拥有独立的 **读协程**（从连接读取消息）和 **写协程**（从 channel 取出消息写入连接）
- 服务端维护一个 `BroadcastChan`，广播协程将消息扇出到所有在线用户的 channel
- 每个用户有一个 **空闲超时协程**，通过 `channel + time.Timer` 实现 100s 空闲检测
- 房间和用户映射使用 `sync.RWMutex` 保护并发访问

### 消息流转

```
Client.Send() ──protobuf──► Server.Handler() ──类型分发──►
  ├─ CHAT     → Broadcast → 所有在线用户
  ├─ PRIVATE  → 目标用户 channel（离线则写入 SQLite）
  ├─ ROOM_*   → ChatRoom 处理方法
  ├─ RENAME   → 更新用户名 → 推送离线消息
  ├─ WHO      → 返回在线用户列表
  ├─ PING     → 回复 PONG
  └─ ACK      → 标记 SQLite 消息已投递
```

---

## 快速开始

### 环境要求

- Go 1.26+

### 构建

```bash
# 克隆仓库
git clone <your-repo-url>
cd golang-IMSystem

# 编译服务端
go build -o server.exe ./cmd/server

# 编译客户端
go build -o client.exe ./cmd/client

# 编译集成测试
go build -o test_integration.exe ./cmd/test_integration
```

### 运行

```bash
# 启动服务端（默认配置）
#   TCP 端口: 8888
#   WebSocket 端口: 8889
#   数据库文件: ./imserver.db
go run ./cmd/server

# 自定义参数
go run ./cmd/server -db ./data/imserver.db -ws 9999

# 启动 CLI 客户端（新终端）
go run ./cmd/client

# 指定服务端地址
go run ./cmd/client -ip 192.168.1.100 -port 8888

# 运行集成测试（需先启动服务端）
go run ./cmd/test_integration
```

---

## 客户端使用

启动客户端后，进入交互菜单：

```
1 - 群聊模式       # 进入公共聊天频道，所有在线用户可见
2 - 私聊模式       # 输入目标用户名，点对点通信
3 - 更新用户名     # 自定义显示名称（重复将提示）
4 - 聊天室菜单     # 进入房间子菜单
0 - 退出          # 断开连接
```

### 聊天室子菜单

```
1 - 创建聊天室     # 输入房间名，创建新房间
2 - 加入聊天室     # 输入房间名，加入已有房间
3 - 离开聊天室     # 输入房间名，离开指定房间
4 - 列出聊天室     # 查看所有活跃房间
5 - 发送房间消息   # 输入房间名和消息内容
0 - 返回主菜单
```

### 群聊 / 私聊模式

进入对应模式后：

- 直接输入文本 → 发送消息
- 输入 `/menu` → 返回主菜单（群聊模式）
- 输入 `/who` → 查看在线用户
- 输入 `/rename <新名字>` → 修改用户名（群聊模式）

---

## 通信协议

### 帧格式

采用 **长度前缀 + Protobuf 二进制** 的编解码方式：

```
┌──────────────────────┬──────────────────────────┐
│  4 bytes (BigEndian) │  N bytes                  │
│  消息体长度          │  Protobuf 序列化消息体     │
└──────────────────────┴──────────────────────────┘
```

### Protobuf 消息定义

```protobuf
syntax = "proto3";
package protocol;

message Message {
  int32  Type       = 1;  // 消息类型
  string From       = 2;  // 发送者用户名
  string To         = 3;  // 接收者用户名（私聊/房间）
  bytes  Content    = 4;  // 消息内容
  int64  CreateTime = 5;  // Unix 时间戳（秒）
  string Room       = 6;  // 房间名（房间消息专用）
}
```

### 消息类型完整列表

| 类型值 | 常量名 | 说明 |
|--------|--------|------|
| 0 | `SYSTEM` | 系统消息（用户上线/下线/被踢等） |
| 1 | `CHAT` | 群聊消息 |
| 2 | `PRIVATE` | 私聊消息 |
| 3 | `WHO` | 查询在线用户请求/响应 |
| 4 | `RENAME` | 修改用户名 |
| 5 | `PING` | 心跳请求 |
| 6 | `PONG` | 心跳响应 |
| 7 | `ROOM_CREATE` | 创建房间 |
| 8 | `ROOM_JOIN` | 加入房间 |
| 9 | `ROOM_LEAVE` | 离开房间 |
| 10 | `ROOM_LIST` | 列出所有房间 |
| 11 | `ROOM_CHAT` | 房间内消息 |
| 12 | `ACK` | 消息送达确认 |

消息类型 0-4 定义在 `message.proto`，5-12 定义在 `message_ext.go`。

---

## 数据库设计

使用 SQLite 单表存储离线消息：

```sql
CREATE TABLE IF NOT EXISTS private_messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    from_user  TEXT NOT NULL,              -- 发送者
    to_user    TEXT NOT NULL,              -- 接收者
    content    BLOB NOT NULL,              -- 消息内容（二进制安全）
    send_time  INTEGER NOT NULL,           -- 发送时间戳
    delivered  INTEGER NOT NULL DEFAULT 0  -- 是否已投递（0/1）
);
```

**工作流程：**

1. 私聊消息到达时，若目标用户不在线 → `INSERT` 到数据库（`delivered=0`）
2. 目标用户上线或改名时 → `SELECT` 未投递消息并推送
3. 客户端收到消息 → 发送 ACK（携带原始时间戳）
4. 服务端收到 ACK → `UPDATE delivered=1`

---

## WebSocket 接入

浏览器或其他 WebSocket 客户端可直接接入：

```javascript
const ws = new WebSocket("ws://127.0.0.1:8889/ws");

ws.onopen = () => {
  // 发送 Protobuf 编码的帧（需与协议格式一致）
  // [4 字节长度前缀] + [Protobuf 二进制]
};

ws.onmessage = (event) => {
  // 解析 Protobuf 帧，处理不同类型的消息
};
```

`ws.go` 将 `gorilla/websocket.Conn` 包装为 `net.Conn` 接口，实现了 `Read`、`Write`、`Close` 等方法，使得上层业务逻辑可以无差别处理 TCP 和 WebSocket 连接。

### WebSocket → net.Conn 适配细节

- `Read()`：从 WebSocket 连接读取下一帧数据，暂存至内部 buffer，按 `io.Reader` 语义逐次返回
- `Write()`：将数据通过 WebSocket 二进制帧发送
- `SetReadDeadline()` / `SetWriteDeadline()`：转换为 PONG 等待和写超时

---

## 开发指南

### 重新生成 Protobuf 代码

```bash
# 安装 protoc 编译器
# https://github.com/protocolbuffers/protobuf/releases

# 安装 Go 插件
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# 生成代码
protoc --go_out=. internal/protocol/message.proto
```

### 项目构建历史

| 提交 | 内容 |
|------|------|
| `3f89a10` | 房间群聊实现 |
| `ad7f536` | 心跳 + 消息持久化 + 断线重连 |
| `4bf3497` | 使用 Protobuf 替代自定义协议 |
| `d78e6f8` | 项目重构（分层架构） |
| `c909eab` | 修复 ListenMessage 未检测 channel 关闭的问题 |
| `b479ae3` | 基础服务端搭建 |

---

## 依赖说明

| 依赖 | 版本 | 用途 |
|------|------|------|
| `google.golang.org/protobuf` | v1.36.11 | Protobuf 序列化与反序列化 |
| `github.com/gorilla/websocket` | v1.5.3 | WebSocket 协议实现 |
| `modernc.org/sqlite` | v1.51.0 | **纯 Go** SQLite 驱动，无需 CGo |

选择 `modernc.org/sqlite` 而非 `mattn/go-sqlite3` 的原因：纯 Go 实现，不依赖系统 C 编译器，跨平台编译零配置。

---

## 许可证

MIT License
