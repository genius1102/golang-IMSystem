package server

import (
	"fmt"
	"imsystem/internal/protocol"
	"net"
	"strings"
	"time"
)

type User struct {
	Name string
	Addr string
	C    chan *protocol.Message
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

// 监听当前user的channel，用 EncodeMessage 写出
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
	// Phase 4: 从所有房间移除
	u.server.MapLock.RLock()
	for _, room := range u.server.Rooms {
		if room.HasMember(u.Name) {
			room.RemoveMember(u.Name)
		}
	}
	u.server.MapLock.RUnlock()

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

// Phase 4: ACK 处理 —— 标记消息已确认
func (u *User) doAck(msg *protocol.Message) {
	if u.server.DB == nil {
		return
	}
	// msg.Content 为原始消息的 timestamp 字符串, msg.To 为原始发送者
	// 通过 (to_user=u.Name, from_user=msg.To, timestamp=msg.Content) 定位并标记已投递
	if err := AckMessage(u.server.DB, u.Name, msg.To, msg.Content); err != nil {
		fmt.Printf("ack message err: %v\n", err)
	}
}

// 处理消息：用 type 字段分发
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

		oldName := u.Name
		u.server.MapLock.Lock()
		delete(u.server.OnlineMap, oldName)
		u.server.OnlineMap[newName] = u
		u.server.MapLock.Unlock()

		u.Name = newName
		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: "rename success: " + newName,
		})

		// Phase 3: 改名后推送离线消息
		u.server.pushOfflineMessages(u)

	// Phase 4: 聊天室操作
	case protocol.MessageType_ROOM_CREATE:
		u.doRoomCreate(msg)

	case protocol.MessageType_ROOM_JOIN:
		u.doRoomJoin(msg)

	case protocol.MessageType_ROOM_LEAVE:
		u.doRoomLeave(msg)

	case protocol.MessageType_ROOM_LIST:
		u.doRoomList(msg)

	case protocol.MessageType_ROOM_CHAT:
		u.doRoomChat(msg)

	// Phase 4: ACK 确认
	case protocol.MessageType_ACK:
		u.doAck(msg)

	// 3. 私聊
	case protocol.MessageType_PRIVATE:
		remoteUser, exists := u.server.OnlineMap[msg.To]
		if !exists {
			// Phase 3: 目标用户不在线，持久化离线消息
			if u.server.DB != nil {
				if err := SaveMessage(u.server.DB, msg); err != nil {
					fmt.Printf("save offline message err: %v\n", err)
				} else {
					fmt.Printf("saved offline message from %s to %s\n", u.Name, msg.To)
				}
			}

			u.SendMsg(&protocol.Message{
				Type:    protocol.MessageType_SYSTEM,
				Content: fmt.Sprintf("user '%s' not online, message saved for offline delivery", msg.To),
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
