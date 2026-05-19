package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
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
	user := NewUser(conn, s)

	// 用户上线
	user.Online()

	// 监听用户是否活跃的管道
	isAlive := make(chan bool)

	// 接收客户端发送消息
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if n == 0 {
				// 用户下线
				user.Offline()
				// 关闭连接
				conn.Close()
				return
			}
			if err != nil && err != io.EOF {
				fmt.Println("Conn Read err:", err)
				return
			}
			msg := strings.TrimSpace(string(buf[:n]))
			user.DoMessage(msg)

			isAlive <- true
		}
	}()

	// handler 阻塞
	for {
		select {
		case <-isAlive:
			// 用户活跃，重置超时时间
			// 不做任何操作，为了激活select，更新下面的定时器

		case <- time.After(time.Second*100):
			// 100秒没有活动，用户强制关闭
			user.SendMsg("you are out")
			close(user.C)
			conn.Close() 

			// 退出当前handler
			return
		}
	}

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
