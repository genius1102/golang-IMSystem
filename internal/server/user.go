package server

import (
	"net"
	"strings"
	"time"

	"imsystem/internal/protocol"
)

type User struct {
	Name string
	Addr string
	C    chan *protocol.Message // 升级为 proto.Message channel
	Conn net.Conn

	server *Server
}

func NewUser(conn net.Conn, server *Server) *User {
	user := &User{
		Name:   conn.RemoteAddr().String(),
		Addr:   conn.RemoteAddr().String(),
		C:      make(chan *protocol.Message),
		Conn:   conn,
		server: server,
	}

	go user.ListenMessage()
	return user
}

// 监听当前user的channel，用 EncodeMessage 写出（长度前缀帧）
func (u *User) ListenMessage() {
	for {
		msg, ok := <-u.C
		if !ok {
			return
		}
		protocol.EncodeMessage(u.Conn, msg)
	}
}

// 用户上线
func (u *User) Online() {
	u.server.MapLock.Lock()
	u.server.OnlineMap[u.Name] = u
	u.server.MapLock.Unlock()

	u.server.BroadCast(&protocol.Message{
		Type:      protocol.MessageType_SYSTEM,
		From:      u.Name,
		Content:   u.Name + " 上线!",
		Timestamp: time.Now().Unix(),
	})
}

// 用户下线
func (u *User) Offline() {
	u.server.MapLock.Lock()
	delete(u.server.OnlineMap, u.Name)
	u.server.MapLock.Unlock()

	u.server.BroadCast(&protocol.Message{
		Type:      protocol.MessageType_SYSTEM,
		From:      u.Name,
		Content:   u.Name + " 下线!",
		Timestamp: time.Now().Unix(),
	})
}

// 给当前用户发送 proto 消息
func (u *User) SendMsg(msg *protocol.Message) {
	protocol.EncodeMessage(u.Conn, msg)
}

// 处理消息：用 type 字段分发，替代原来的字符串前缀匹配
func (u *User) DoMessage(msg *protocol.Message) {

	// 服务端自动填充发送者和时间戳
	msg.From = u.Name
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().Unix()
	}

	switch msg.Type {

	// 1. 查询在线用户
	case protocol.MessageType_WHO:
		u.server.MapLock.Lock()
		var users []string
		for _, user := range u.server.OnlineMap {
			users = append(users, "["+user.Addr+"]"+user.Name+":is online...")
		}
		u.server.MapLock.Unlock()

		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: strings.Join(users, "\n"),
		})

	// 2. 修改用户名
	case protocol.MessageType_RENAME:
		newName := msg.Content

		if _, exists := u.server.OnlineMap[newName]; exists {
			u.SendMsg(&protocol.Message{
				Type:    protocol.MessageType_SYSTEM,
				Content: "rename error: name already exists",
			})
			return
		}

		u.server.MapLock.Lock()
		delete(u.server.OnlineMap, u.Name)
		u.server.OnlineMap[newName] = u
		u.server.MapLock.Unlock()

		u.Name = newName
		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: "rename success: " + newName,
		})

	// 3. 私聊
	case protocol.MessageType_PRIVATE:
		remoteUser, exists := u.server.OnlineMap[msg.To]
		if !exists {
			u.SendMsg(&protocol.Message{
				Type:    protocol.MessageType_SYSTEM,
				Content: "user not online",
			})
			return
		}
		remoteUser.SendMsg(&protocol.Message{
			Type:      protocol.MessageType_PRIVATE,
			From:      u.Name,
			Content:   msg.Content,
			Timestamp: msg.Timestamp,
		})

	// 4. 群聊（默认）
	default:
		u.server.BroadCast(msg)
	}
}
