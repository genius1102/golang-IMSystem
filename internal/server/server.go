package server

import (
	"bufio"
	"fmt"
	"imsystem/internal/protocol"
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

	// 优雅关闭
	Quit chan struct{}
}

// 创建一个server接口
func NewServer(ip string, port int) *Server {
	server := &Server{
		Ip:        ip,
		Port:      port,
		OnlineMap: make(map[string]*User),
		Message:   make(chan string),
		Quit:      make(chan struct{}),
	}
	return server
}

// 监听广播消息的goroutine，一旦有消息就广播给所有User
func (s *Server) ListenMessage() {
	for {
		select {
		case msg := <-s.Message:
			// 将消息广播给所有在线User
			s.MapLock.Lock()
			for _, user := range s.OnlineMap {
				user.C <- msg
			}
			s.MapLock.Unlock()

		case <-s.Quit:
			return
		}
	}
}

// 广播消息方法
func (s *Server) BroadCast(user *User, message string) {
	sendMsg := protocol.BuildSystemMsg(user.Addr, user.Name, message)
	s.Message <- sendMsg
}

func (s *Server) Handler(conn net.Conn) {
	// 创建用户
	user := NewUser(conn, s)

	// 用户上线
	user.Online()

	// 监听用户是否活跃的管道
	isAlive := make(chan bool)

	// 接收客户端发送消息（使用 bufio 逐行读取，解决粘包问题）
	go func() {
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Println("Conn Read err:", err)
				}
				// 用户下线
				user.Offline()
				conn.Close()
				return
			}

			msg := strings.TrimSpace(line)
			if msg != "" {
				user.DoMessage(msg)
			}

			isAlive <- true
		}
	}()

	// 超时检测（使用 time.NewTimer 避免 time.After 泄漏）
	timer := time.NewTimer(time.Second * 100)
	for {
		select {
		case <-isAlive:
			// 用户活跃，重置定时器
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(time.Second * 100)

		case <-timer.C:
			// 超时，强制关闭
			user.SendMsg("you are out")
			close(user.C)
			conn.Close()

			// 从在线列表中移除
			s.MapLock.Lock()
			delete(s.OnlineMap, user.Name)
			s.MapLock.Unlock()

			return

		case <-s.Quit:
			// 服务器关闭，通知用户并退出
			user.SendMsg("server is shutting down\n")
			close(user.C)
			conn.Close()
			return
		}
	}
}

// 启动服务器
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", net.JoinHostPort(s.Ip, fmt.Sprintf("%d", s.Port)))
	if err != nil {
		return fmt.Errorf("listen failed: %w", err)
	}
	defer listener.Close()

	fmt.Printf("Server started at %s:%d\n", s.Ip, s.Port)

	// 启动监听广播消息的goroutine
	go s.ListenMessage()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// 检查是否因为关闭而退出
			select {
			case <-s.Quit:
				return nil
			default:
				fmt.Println("listener accept error:", err)
				continue
			}
		}

		// 处理连接
		go s.Handler(conn)
	}
}

// 优雅关闭服务器
func (s *Server) Stop() {
	fmt.Println("\nServer is shutting down...")

	// 通知所有goroutine退出
	close(s.Quit)

	// 通知所有在线用户
	s.MapLock.Lock()
	for _, user := range s.OnlineMap {
		user.SendMsg("server is shutting down\n")
		close(user.C)
		user.Conn.Close()
	}
	// 清空在线列表
	s.OnlineMap = make(map[string]*User)
	s.MapLock.Unlock()

	fmt.Println("Server stopped.")
}
