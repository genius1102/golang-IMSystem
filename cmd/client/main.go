package main

import (
	"bufio"
	"flag"
	"fmt"
	"imsystem/internal/protocol"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	ServerIP   string
	ServerPort int
	Name       string
	Conn       net.Conn
	flag       int
	reader     *bufio.Reader

	// Phase 3: 连接管理
	connected  atomic.Bool // 连接状态标志
	lastPong   time.Time   // 最后一次收到 PONG 的时间
	pongMu     sync.Mutex
	disconnect chan struct{} // 断线信号
	reconnected chan struct{} // 重连完成信号
}

func NewClient(serverIp string, serverPort int) *Client {
	client := &Client{
		ServerIP:    serverIp,
		ServerPort:  serverPort,
		flag:        999,
		reader:      bufio.NewReader(os.Stdin),
		disconnect:  make(chan struct{}, 1),
		reconnected: make(chan struct{}, 1),
	}

	conn, err := net.Dial("tcp", net.JoinHostPort(serverIp, fmt.Sprintf("%d", serverPort)))
	if err != nil {
		fmt.Println("Dial server failed:", err)
		return nil
	}

	client.Conn = conn
	client.connected.Store(true)
	client.pongMu.Lock()
	client.lastPong = time.Now()
	client.pongMu.Unlock()

	return client
}

// 读取一行用户输入，同时检测连接状态
func (c *Client) readLine(prompt string) string {
	fmt.Print(prompt)
	input, err := c.reader.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSpace(input)
}

// 检测是否已连接
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// Phase 3: 心跳 goroutine —— 每 30 秒发送 PING，检测 PONG 超时
func (c *Client) Heartbeat() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.IsConnected() {
				return
			}

			// 发送 PING
			if err := protocol.EncodeMessage(c.Conn, &protocol.Message{
				Type: protocol.MessageType_PING,
			}); err != nil {
				fmt.Println("\n>>>>>> 心跳发送失败:", err)
				c.signalDisconnect()
				return
			}

			// 检查是否超过 90 秒没收到 PONG
			c.pongMu.Lock()
			since := time.Since(c.lastPong)
			c.pongMu.Unlock()
			if since > 90*time.Second {
				fmt.Println("\n>>>>>> 心跳超时（90秒未收到PONG），连接可能已断开")
				c.signalDisconnect()
				return
			}

		case <-c.disconnect:
			return
		}
	}
}

// 发送断线信号
func (c *Client) signalDisconnect() {
	if c.connected.CompareAndSwap(true, false) {
		select {
		case c.disconnect <- struct{}{}:
		default:
		}
	}
}

// 接收服务器消息：解码 proto 消息 → 格式化显示
func (c *Client) ReceiveMessage() {
	for {
		if !c.IsConnected() {
			return
		}

		msg, err := protocol.DecodeMessage(c.Conn)
		if err != nil {
			fmt.Println("\n>>>>>> 与服务器的连接已断开:", err)
			c.signalDisconnect()
			return
		}

		// Phase 3: 处理 PONG 响应
		if msg.Type == protocol.MessageType_PONG {
			c.pongMu.Lock()
			c.lastPong = time.Now()
			c.pongMu.Unlock()
			continue
		}

		// 根据消息类型格式化输出
		switch msg.Type {
		case protocol.MessageType_SYSTEM:
			fmt.Println(msg.Content)

		case protocol.MessageType_CHAT:
			fmt.Printf("[%s]:%s\n", msg.From, msg.Content)

		case protocol.MessageType_PRIVATE:
			fmt.Printf("%s send to you: %s\n", msg.From, msg.Content)

		default:
			fmt.Println(msg.Content)
		}
	}
}

// 发送 proto 消息（线程安全）
func (c *Client) sendMessage(msg *protocol.Message) {
	if !c.IsConnected() {
		fmt.Println(">>>>>> 未连接到服务器，无法发送消息")
		return
	}
	if err := protocol.EncodeMessage(c.Conn, msg); err != nil {
		fmt.Println("send message err:", err)
		c.signalDisconnect()
	}
}

// Phase 3: 断线重连（指数退避）
func (c *Client) reconnect() bool {
	for attempt := 1; attempt <= 10; attempt++ {
		backoff := time.Duration(math.Min(30, math.Pow(2, float64(attempt)))) * time.Second
		fmt.Printf(">>>>>> 尝试重新连接 (%d/10), 等待 %v...\n", attempt, backoff)
		time.Sleep(backoff)

		addr := net.JoinHostPort(c.ServerIP, fmt.Sprintf("%d", c.ServerPort))
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			fmt.Printf(">>>>>> 连接失败: %v\n", err)
			continue
		}

		// 关闭旧连接（如果还有效）
		if c.Conn != nil {
			c.Conn.Close()
		}
		c.Conn = conn
		c.connected.Store(true)

		// 恢复 PONG 计时器
		c.pongMu.Lock()
		c.lastPong = time.Now()
		c.pongMu.Unlock()

		// 重新注册用户名
		if c.Name != "" && c.Name != c.Conn.RemoteAddr().String() {
			if err := protocol.EncodeMessage(c.Conn, &protocol.Message{
				Type:    protocol.MessageType_RENAME,
				Content: c.Name,
			}); err != nil {
				fmt.Printf(">>>>>> 重新注册用户名失败: %v\n", err)
				c.connected.Store(false)
				continue
			}
		}

		fmt.Println(">>>>>> 重新连接成功!")
		return true
	}

	fmt.Println(">>>>>> 重连失败，已达最大重试次数")
	return false
}

// 显示菜单
func (c *Client) Menu() bool {
	fmt.Println("1、群聊模式")
	fmt.Println("2、私聊模式")
	fmt.Println("3、更新用户名")
	fmt.Println("0、退出")

	input := c.readLine("")

	switch input {
	case "1":
		c.flag = 1
		return true
	case "2":
		c.flag = 2
		return true
	case "3":
		c.flag = 3
		return true
	case "0":
		c.flag = 0
		return true
	default:
		fmt.Println(">>>>>>Invalid choice, please try again.")
		return false
	}
}

// 群聊模式
func (c *Client) GroupChat() {
	fmt.Println(">>>>>>please input chat message (exit out)")

	for {
		chatMsg := c.readLine("")
		if chatMsg == "exit" {
			return
		}
		if chatMsg == "" {
			if !c.IsConnected() {
				return // 断线时返回主循环处理重连
			}
			continue
		}

		c.sendMessage(&protocol.Message{
			Type:    protocol.MessageType_CHAT,
			Content: chatMsg,
		})
	}
}

// 在线用户列表
func (c *Client) OnlineUserList() {
	c.sendMessage(&protocol.Message{
		Type: protocol.MessageType_WHO,
	})
}

// 私聊模式
func (c *Client) PrivateChat() {
	c.OnlineUserList()

	remoteName := c.readLine(">>>>>>请输入聊天对象用户名（输入exit退出）: ")
	if remoteName == "exit" || remoteName == "" {
		return
	}

	for {
		chatMsg := c.readLine(">>>>>>请输入消息内容（输入exit退出）：")
		if chatMsg == "exit" {
			break
		}
		if chatMsg == "" {
			if !c.IsConnected() {
				break // 断线时返回主循环处理重连
			}
			continue
		}

		c.sendMessage(&protocol.Message{
			Type:    protocol.MessageType_PRIVATE,
			To:      remoteName,
			Content: chatMsg,
		})
	}
}

// 更新用户名
func (c *Client) UpdateName() bool {
	newName := c.readLine(">>>>>>Please input rename: ")
	if newName == "" {
		return false
	}

	c.Name = newName
	c.sendMessage(&protocol.Message{
		Type:    protocol.MessageType_RENAME,
		Content: newName,
	})
	return true
}

// 主业务
func (c *Client) Run() {
	// 启动心跳
	go c.Heartbeat()

	// 启动消息接收
	go c.ReceiveMessage()

	for c.flag != 0 {
		// Phase 3: 检测断线 → 自动重连
		if !c.IsConnected() {
			fmt.Println("\n>>>>>> 连接已断开，开始重连...")
			if !c.reconnect() {
				fmt.Println(">>>>>> 重连失败，程序退出")
				return
			}
			// 重连成功后重启接收和心跳协程
			go c.ReceiveMessage()
			go c.Heartbeat()
			fmt.Println(">>>>>> 已恢复，继续操作...")
		}

		for c.Menu() != true {
		}

		switch c.flag {
		case 1:
			c.GroupChat()
		case 2:
			c.PrivateChat()
		case 3:
			c.UpdateName()
		case 0:
			fmt.Println("退出")
		}
	}

	// 关闭连接
	if c.Conn != nil {
		c.Conn.Close()
	}
}

var serverIp string
var serverPort int

func init() {
	flag.StringVar(&serverIp, "ip", "127.0.0.1", "server ip")
	flag.IntVar(&serverPort, "port", 8888, "server port")
}

func main() {
	flag.Parse()
	client := NewClient(serverIp, serverPort)
	if client == nil {
		fmt.Println(">>>>>>>>failed connect to server......")
		return
	}

	fmt.Println(">>>>>>>>success connect to server......")

	client.Run()
}
