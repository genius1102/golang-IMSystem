package server

import (
	"database/sql"
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

	// Phase 3: SQLite 数据库
	DB *sql.DB

	// Phase 4: 聊天室管理
	Rooms map[string]*Room
}

func NewServer(ip string, port int, db *sql.DB) *Server {
	return &Server{
		Ip:        ip,
		Port:      port,
		OnlineMap: make(map[string]*User),
		Message:   make(chan *protocol.Message),
		Quit:      make(chan struct{}),
		DB:        db,
		Rooms:     make(map[string]*Room),
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

// Phase 3: 推送离线消息给刚上线的用户
func (s *Server) pushOfflineMessages(user *User) {
	if s.DB == nil {
		return
	}

	msgs, err := GetUndeliveredMessages(s.DB, user.Name)
	if err != nil {
		fmt.Printf("get undelivered messages for %s err: %v\n", user.Name, err)
		return
	}

	if len(msgs) == 0 {
		return
	}

	// 先发一条系统通知
	user.SendMsg(&protocol.Message{
		Type:    protocol.MessageType_SYSTEM,
		Content: fmt.Sprintf(">>>>>> 您有 %d 条离线消息:", len(msgs)),
	})

	// 逐条推送
	for _, msg := range msgs {
		user.SendMsg(msg)
	}

	// 标记为已投递
	if err := MarkMessagesDelivered(s.DB, user.Name); err != nil {
		fmt.Printf("mark delivered for %s err: %v\n", user.Name, err)
	}

	fmt.Printf("pushed %d offline messages to %s\n", len(msgs), user.Name)
}

func (s *Server) Handler(conn net.Conn) {
	user := NewUser(conn, s)
	user.Online()

	// Phase 3: 推送离线消息
	s.pushOfflineMessages(user)

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

			// Phase 3: 心跳 PING → 直接回复 PONG
			if msg.Type == protocol.MessageType_PING {
				user.SendMsg(&protocol.Message{
					Type:      protocol.MessageType_PONG,
					Timestamp: time.Now().Unix(),
				})
				isAlive <- true
				continue
			}

			// Phase 3: PONG 消息只需刷新活跃状态
			if msg.Type == protocol.MessageType_PONG {
				isAlive <- true
				continue
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

	// Phase 3: 关闭数据库
	if s.DB != nil {
		s.DB.Close()
	}

	fmt.Println("Server stopped.")
}
