package server

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"imsystem/internal/protocol"
)

// Room 聊天室
type Room struct {
	Name      string
	Creator   string
	Members   map[string]bool // 用户名 → 是否在线
	CreatedAt time.Time
	mu        sync.RWMutex
}

// NewRoom 创建聊天室
func NewRoom(name, creator string) *Room {
	return &Room{
		Name:      name,
		Creator:   creator,
		Members:   make(map[string]bool),
		CreatedAt: time.Now(),
	}
}

// AddMember 添加成员
func (r *Room) AddMember(username string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Members[username] = true
}

// RemoveMember 移除成员
func (r *Room) RemoveMember(username string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.Members, username)
}

// HasMember 检查是否成员
func (r *Room) HasMember(username string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Members[username]
}

// GetMembers 获取成员列表
func (r *Room) GetMembers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	members := make([]string, 0, len(r.Members))
	for m := range r.Members {
		members = append(members, m)
	}
	return members
}

// MemberCount 成员数量
func (r *Room) MemberCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.Members)
}

// ---------- Server 房间管理方法 ----------

// CreateRoom 创建聊天室
func (s *Server) CreateRoom(name, creator string) error {
	s.MapLock.Lock()
	defer s.MapLock.Unlock()

	// Phase 4: 使用 Rooms map（延迟初始化）
	if _, exists := s.Rooms[name]; exists {
		return fmt.Errorf("room '%s' already exists", name)
	}

	room := NewRoom(name, creator)
	room.AddMember(creator) // 创建者自动加入
	s.Rooms[name] = room

	fmt.Printf("room '%s' created by %s\n", name, creator)
	return nil
}

// JoinRoom 加入聊天室
func (s *Server) JoinRoom(roomName, username string) error {
	// 检查房间是否存在
	if room := s.GetRoom(roomName); room != nil {
		room.AddMember(username)
		fmt.Printf("%s joined room '%s'\n", username, roomName)
		return nil
	}
	return fmt.Errorf("room '%s' not found", roomName)
}

// LeaveRoom 离开聊天室
func (s *Server) LeaveRoom(roomName, username string) error {
	room := s.GetRoom(roomName)
	if room == nil {
		return fmt.Errorf("room '%s' not found", roomName)
	}
	room.RemoveMember(username)
	fmt.Printf("%s left room '%s'\n", username, roomName)

	// 如果房间空了，删除房间
	if room.MemberCount() == 0 {
		s.MapLock.Lock()
		delete(s.Rooms, roomName)
		s.MapLock.Unlock()
		fmt.Printf("room '%s' deleted (empty)\n", roomName)
	}
	return nil
}

// GetRoom 获取聊天室
func (s *Server) GetRoom(name string) *Room {
	s.MapLock.RLock()
	defer s.MapLock.RUnlock()
	return s.Rooms[name]
}

// ListRooms 列出所有聊天室
func (s *Server) ListRooms() []string {
	s.MapLock.RLock()
	defer s.MapLock.RUnlock()

	rooms := make([]string, 0, len(s.Rooms))
	for name, room := range s.Rooms {
		rooms = append(rooms, fmt.Sprintf("[%s] creator=%s members=%d",
			name, room.Creator, room.MemberCount()))
	}
	return rooms
}

// BroadCastToRoom 向房间内所有在线成员广播消息
func (s *Server) BroadCastToRoom(roomName string, msg *protocol.Message) {
	room := s.GetRoom(roomName)
	if room == nil {
		return
	}

	s.MapLock.RLock()
	defer s.MapLock.RUnlock()

	for member := range room.Members {
		if user, online := s.OnlineMap[member]; online {
			user.C <- msg
		}
	}
}

// ---------- User 房间相关消息处理 ----------

// doRoomCreate 处理创建聊天室请求
func (u *User) doRoomCreate(msg *protocol.Message) {
	roomName := strings.TrimSpace(msg.Content)
	if roomName == "" {
		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: "usage: enter room name to create",
		})
		return
	}

	if err := u.server.CreateRoom(roomName, u.Name); err != nil {
		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: fmt.Sprintf("create room failed: %v", err),
		})
		return
	}

	u.SendMsg(&protocol.Message{
		Type:    protocol.MessageType_SYSTEM,
		Content: fmt.Sprintf("room '%s' created, you are in", roomName),
	})

	// 广播给所有人房间创建消息
	u.server.BroadCast(&protocol.Message{
		Type:      protocol.MessageType_SYSTEM,
		From:      u.Name,
		Content:   fmt.Sprintf("%s created a new room: '%s'", u.Name, roomName),
		Timestamp: time.Now().Unix(),
	})
}

// doRoomJoin 处理加入聊天室请求
func (u *User) doRoomJoin(msg *protocol.Message) {
	roomName := strings.TrimSpace(msg.Content)

	if err := u.server.JoinRoom(roomName, u.Name); err != nil {
		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: fmt.Sprintf("join room failed: %v", err),
		})
		return
	}

	u.SendMsg(&protocol.Message{
		Type:    protocol.MessageType_SYSTEM,
		Content: fmt.Sprintf("joined room '%s'", roomName),
	})

	// 通知房间内其他人
	u.server.BroadCastToRoom(roomName, &protocol.Message{
		Type:      protocol.MessageType_SYSTEM,
		From:      u.Name,
		Content:   fmt.Sprintf("%s joined the room", u.Name),
		Timestamp: time.Now().Unix(),
	})
}

// doRoomLeave 处理离开聊天室请求
func (u *User) doRoomLeave(msg *protocol.Message) {
	roomName := strings.TrimSpace(msg.Content)

	// 先通知房间内其他人
	u.server.BroadCastToRoom(roomName, &protocol.Message{
		Type:      protocol.MessageType_SYSTEM,
		From:      u.Name,
		Content:   fmt.Sprintf("%s left the room", u.Name),
		Timestamp: time.Now().Unix(),
	})

	if err := u.server.LeaveRoom(roomName, u.Name); err != nil {
		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: fmt.Sprintf("leave room failed: %v", err),
		})
		return
	}

	u.SendMsg(&protocol.Message{
		Type:    protocol.MessageType_SYSTEM,
		Content: fmt.Sprintf("left room '%s'", roomName),
	})
}

// doRoomList 处理查询聊天室列表请求
func (u *User) doRoomList(msg *protocol.Message) {
	rooms := u.server.ListRooms()
	if len(rooms) == 0 {
		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: "no rooms available",
		})
		return
	}

	u.SendMsg(&protocol.Message{
		Type:    protocol.MessageType_SYSTEM,
		Content: "=== Rooms ===\n" + strings.Join(rooms, "\n"),
	})
}

// doRoomChat 处理聊天室消息
func (u *User) doRoomChat(msg *protocol.Message) {
	roomName := strings.TrimSpace(msg.To) // To 字段存放房间名

	room := u.server.GetRoom(roomName)
	if room == nil {
		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: fmt.Sprintf("room '%s' not found", roomName),
		})
		return
	}

	if !room.HasMember(u.Name) {
		u.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: fmt.Sprintf("you are not in room '%s', join first", roomName),
		})
		return
	}

	// 向房间内所有在线成员广播
	u.server.BroadCastToRoom(roomName, &protocol.Message{
		Type:      protocol.MessageType_ROOM_CHAT,
		From:      u.Name,
		Content:   msg.Content,
		Timestamp: time.Now().Unix(),
	})
}
