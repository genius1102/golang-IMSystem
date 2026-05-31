package server

import (
	"net"

	"imsystem/internal/protocol"
)

type User struct {
	Name string
	Addr string
	C    chan string
	Conn net.Conn

	server *Server
}

// 创建一个用户的API
func NewUser(conn net.Conn, server *Server) *User {
	userAddr := conn.RemoteAddr().String()
	user := &User{
		Name:   userAddr,
		Addr:   userAddr,
		C:      make(chan string),
		Conn:   conn,
		server: server,
	}

	// 启动监听当前user的channel的goroutine
	go user.ListenMessage()

	return user
}

// 监听当前user的channel，一旦有消息直接发送给客户端
func (u *User) ListenMessage() {
	for {
		msg, ok := <-u.C
		if !ok {
			return // channel关闭，goroutine安全退出
		}
		u.Conn.Write([]byte(msg + "\n"))
	}
}

// 用户上线业务
func (u *User) Online() {
	u.server.MapLock.Lock()
	u.server.OnlineMap[u.Name] = u
	u.server.MapLock.Unlock()

	// 广播用户上线消息
	u.server.BroadCast(u, " online !")
}

// 用户下线业务
func (u *User) Offline() {
	u.server.MapLock.Lock()
	delete(u.server.OnlineMap, u.Name)
	u.server.MapLock.Unlock()

	// 广播用户下线消息
	u.server.BroadCast(u, " offline !")
}

// 给当前用户的客户端发消息
func (u *User) SendMsg(msg string) {
	u.Conn.Write([]byte(msg))
}

// 用户处理消息业务
func (u *User) DoMessage(msg string) {

	// 1. 查询在线用户
	if protocol.IsWhoCmd(msg) {
		u.server.MapLock.Lock()
		for _, user := range u.server.OnlineMap {
			onlineMsg := "[" + user.Addr + "]" + user.Name + ":is online...\n"
			u.SendMsg(onlineMsg)
		}
		u.server.MapLock.Unlock()
		return
	}

	// 2. 修改用户名: rename|newName
	if newName, ok := protocol.ParseRenameMsg(msg); ok {
		if _, exists := u.server.OnlineMap[newName]; exists {
			u.SendMsg("rename error: name already exists\n")
			return
		}

		u.server.MapLock.Lock()
		delete(u.server.OnlineMap, u.Name)
		u.server.OnlineMap[newName] = u
		u.server.MapLock.Unlock()

		u.Name = newName
		u.SendMsg("rename success: " + newName + "\n")
		return
	}

	// 3. 私聊: to|targetName|content
	if target, content, ok := protocol.ParsePrivateMsg(msg); ok {
		remoteUser, exists := u.server.OnlineMap[target]
		if !exists {
			u.SendMsg("user not online\n")
			return
		}
		remoteUser.SendMsg(u.Name + " send to you: " + content + "\n")
		return
	}

	// 4. 默认：群聊广播
	u.server.BroadCast(u, ":"+msg)
}
