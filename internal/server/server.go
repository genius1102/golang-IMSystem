package server

import (
	"fmt"
	"imsystem/internal/protocol"
	"io"
	"net"
	"sync"
	"time"
)

type Server struct {
	Ip   string
	Port int

	// 在线用户列表
	OnlineMap map[string]*User
	MapLock   sync.RWMutex

	// 广播消息的channel（改为 proto.Message）
	Message chan *protocol.Message

	// 优雅关闭
	Quit chan struct{}
}

func NewServer(ip string, port int) *Server {
	return &Server{
		Ip:        ip,
		Port:      port,
		OnlineMap: make(map[string]*User),
		Message:   make(chan *protocol.Message),
		Quit:      make(chan struct{}),
	}
}

// 监听广播消息的goroutine
func (s *Server) ListenMessage() {
	for {
		select {
		case msg := <-s.Message:
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

// 广播消息
func (s *Server) BroadCast(msg *protocol.Message) {
	s.Message <- msg
}

func (s *Server) Handler(conn net.Conn) {
	user := NewUser(conn, s)
	user.Online()

	isAlive := make(chan bool)

	// 接收客户端消息：用长度前缀帧读取（替代 bufio.ReadString）
	go func() {
		for {
			msg, err := protocol.DecodeMessage(conn)
			if err != nil {
				if err != io.EOF {
					fmt.Println("Conn Read err:", err)
				}
				user.Offline()
				conn.Close()
				return
			}

			user.DoMessage(msg)
			isAlive <- true
		}
	}()

	// 超时检测
	timer := time.NewTimer(time.Second * 100)
	for {
		select {
		case <-isAlive:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(time.Second * 100)

		case <-timer.C:
			user.SendMsg(&protocol.Message{
				Type:    protocol.MessageType_SYSTEM,
				Content: "you are out",
			})
			close(user.C)
			conn.Close()

			s.MapLock.Lock()
			delete(s.OnlineMap, user.Name)
			s.MapLock.Unlock()
			return

		case <-s.Quit:
			user.SendMsg(&protocol.Message{
				Type:    protocol.MessageType_SYSTEM,
				Content: "server is shutting down",
			})
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

	go s.ListenMessage()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.Quit:
				return nil
			default:
				fmt.Println("listener accept error:", err)
				continue
			}
		}

		go s.Handler(conn)
	}
}

// 优雅关闭服务器
func (s *Server) Stop() {
	fmt.Println("\nServer is shutting down...")

	close(s.Quit)

	// 通知所有在线用户
	s.MapLock.Lock()
	for _, user := range s.OnlineMap {
		user.SendMsg(&protocol.Message{
			Type:    protocol.MessageType_SYSTEM,
			Content: "server is shutting down",
		})
		close(user.C)
		user.Conn.Close()
	}
	s.OnlineMap = make(map[string]*User)
	s.MapLock.Unlock()

	fmt.Println("Server stopped.")
}
