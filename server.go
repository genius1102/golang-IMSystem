package main

import (
	"fmt"
	"net"
	"sync"
)

type Server struct {
	Ip   string
	Port int

	// 在线用户列表
	OnlineMap map[string]*User
	MapLock   sync.RWMutex

	// 广播消息的channel
	Message chan string
}

// 创建一个server接口
func NewServer(ip string, port int) *Server {
	server := &Server{
		Ip:        ip,
		Port:      port,
		OnlineMap: make(map[string]*User),
		Message:   make(chan string),
	}
	return server
}

// 监听广播消息的goroutine,一旦有消息就广播给所有User
func (s *Server) ListenMessage() {
	for {
		msg := <-s.Message

		// 将消息广播给所有在线User
		s.MapLock.Lock()

		for _, user := range s.OnlineMap {
			user.C <- msg
		}

		s.MapLock.Unlock()

	}
}

// 广播消息方法
func (s *Server) BroadCast(user *User, message string) {
	sendMsg := "[" + user.Addr + "]" + user.Name + message
	s.Message <- sendMsg
}

func (s *Server) Handler(conn net.Conn) {
	// 当前连接的业务
	// fmt.Println("连接建立成功")

	// 创建用户
	user := NewUser(conn)

	// 用户上线将其放到onlinemap中
	// 加锁
	s.MapLock.Lock()

	s.OnlineMap[user.Name] = user

	s.MapLock.Unlock()

	// 广播用户上线消息
	s.BroadCast(user, " is online !")

	// handler 阻塞
	select {}

}

// 启动服务器的接口
func (s *Server) Start() {
	//socket listen 监听端口
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.Ip, s.Port))
	if err != nil {
		fmt.Println("listen failed:", err)
	}
	//close listen socket 关闭监听端口
	defer listener.Close()

	// 启动监听广播消息的goroutine
	go s.ListenMessage()

	for {
		//accept 接受连接
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("listener accept error:", err)
			continue
		}

		//do handler 处理连接
		go s.Handler(conn)
	}

}
