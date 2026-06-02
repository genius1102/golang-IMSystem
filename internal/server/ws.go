package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"imsystem/internal/protocol"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // 允许所有来源（开发环境）
	},
}

// wsConn 将 *websocket.Conn 适配为类似 net.Conn 的接口
// 实现 io.Reader, io.Writer, RemoteAddr(), Close()
type wsConn struct {
	conn       *websocket.Conn
	remoteAddr string
	reader     io.Reader // 当前消息的 reader
	readMu     sync.Mutex
	writeMu    sync.Mutex
}

func newWSConn(conn *websocket.Conn, remoteAddr string) *wsConn {
	return &wsConn{
		conn:       conn,
		remoteAddr: remoteAddr,
	}
}

// Read 实现 io.Reader —— 从 WebSocket 读取一条消息
func (w *wsConn) Read(p []byte) (int, error) {
	w.readMu.Lock()
	defer w.readMu.Unlock()

	// 如果当前消息还没读完，继续从 reader 读
	if w.reader == nil {
		_, r, err := w.conn.NextReader()
		if err != nil {
			return 0, err
		}
		w.reader = r
	}

	n, err := w.reader.Read(p)
	if err == io.EOF {
		w.reader = nil // 当前消息读完，标记为 nil 等待下一条
	}
	return n, err
}

// Write 实现 io.Writer —— 向 WebSocket 写入一条二进制消息
func (w *wsConn) Write(p []byte) (int, error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	writer, err := w.conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return 0, err
	}
	defer writer.Close()

	n, err := writer.Write(p)
	return n, err
}

// Close 关闭 WebSocket 连接
func (w *wsConn) Close() error {
	return w.conn.Close()
}

// RemoteAddr 返回远程地址
func (w *wsConn) RemoteAddr() net.Addr {
	return &dummyAddr{addr: w.remoteAddr}
}

// LocalAddr 实现 net.Conn
func (w *wsConn) LocalAddr() net.Addr {
	return &dummyAddr{addr: "ws-server"}
}

// SetDeadline 实现 net.Conn
func (w *wsConn) SetDeadline(t time.Time) error {
	w.conn.SetReadDeadline(t)
	return w.conn.SetWriteDeadline(t)
}

// SetReadDeadline 实现 net.Conn
func (w *wsConn) SetReadDeadline(t time.Time) error {
	return w.conn.SetReadDeadline(t)
}

// SetWriteDeadline 实现 net.Conn
func (w *wsConn) SetWriteDeadline(t time.Time) error {
	return w.conn.SetWriteDeadline(t)
}

// dummyAddr 实现 net.Addr
type dummyAddr struct {
	addr string
}

func (a *dummyAddr) Network() string { return "ws" }
func (a *dummyAddr) String() string  { return a.addr }



// StartWebSocket 启动 WebSocket 服务器
func (s *Server) StartWebSocket(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWebSocket)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	fmt.Printf("WebSocket server started at ws://%s/ws\n", addr)

	go func() {
		<-s.Quit
		server.Close()
	}()

	return server.ListenAndServe()
}

// handleWebSocket 处理 WebSocket 升级和消息收发
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("WebSocket upgrade err:", err)
		return
	}

	remoteAddr := r.RemoteAddr
	ws := newWSConn(conn, remoteAddr)

	user := &User{
		Name:   remoteAddr,
		Addr:   remoteAddr,
		C:      make(chan *protocol.Message),
		Conn:   ws,
		server: s,
	}

	// 启动写协程
	go user.ListenMessage()

	// 上线
	user.Online()
	s.pushOfflineMessages(user)

	fmt.Printf("WebSocket user connected: %s\n", remoteAddr)

	// 读循环（类似 TCP Handler 中的读取协程）
	go func() {
		defer func() {
			user.Offline()
			conn.Close()
			fmt.Printf("WebSocket user disconnected: %s\n", remoteAddr)
		}()

		for {
			msg, err := protocol.DecodeMessage(ws)
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					fmt.Printf("WS read err for %s: %v\n", remoteAddr, err)
				}
				return
			}

			// PING → PONG
			if msg.Type == protocol.MessageType_PING {
				user.SendMsg(&protocol.Message{
					Type:      protocol.MessageType_PONG,
					Timestamp: time.Now().Unix(),
				})
				continue
			}

			// PONG → 忽略（WebSocket 自带 ping/pong，此处仅作兼容）
			if msg.Type == protocol.MessageType_PONG {
				continue
			}

			user.DoMessage(msg)
		}
	}()

	// 阻塞直到 Quit
	<-s.Quit
	conn.Close()
}
