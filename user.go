package main

import "net"

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
		Name: userAddr,
		Addr: userAddr,
		C:    make(chan string),
		Conn: conn,

		server: server,
	}

	// 启动监听当前user的channel 的goroutine
	go user.ListenMessage()

	return user
}

// 监听当前user的channel，一旦有消息直接发送给客户端
func (u *User) ListenMessage() {
	for {
		msg := <-u.C
		u.Conn.Write([]byte(msg + "\n"))
	}
}

// 用户上线业务
func (u *User) Online() {
	// 用户上线将其放到onlinemap中
	// 加锁
	u.server.MapLock.Lock()

	u.server.OnlineMap[u.Name] = u

	u.server.MapLock.Unlock()

	// 广播用户上线消息
	u.server.BroadCast(u, " online !")
}

// 用户下线业务
func (u *User) Offline() {
	// 用户下线将其从onlinemap中删除
	// 加锁
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

	if msg == "who" {
		// 查询所有在线用户
		u.server.MapLock.Lock()
		for _, user := range u.server.OnlineMap {
			olineMessage := "[" + user.Addr + "]" + user.Name + ":" + "is online...\n"
			u.SendMsg(olineMessage)
		}
		u.server.MapLock.Unlock()
	} else {
		// 有消息就广播
		u.server.BroadCast(u, msg)
	}

}
