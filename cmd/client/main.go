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
	connected   atomic.Bool
	lastPong    time.Time
	pongMu      sync.Mutex
	disconnect  chan struct{}
	reconnected chan struct{}
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

func (c *Client) readLine(prompt string) string {
	fmt.Print(prompt)
	input, err := c.reader.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSpace(input)
}

func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// Phase 4: 发送 ACK 确认
func (c *Client) sendAck(msg *protocol.Message) {
	// 只对私聊和聊天室消息发 ACK（系统消息不需要）
	if msg.Type != protocol.MessageType_PRIVATE && msg.Type != protocol.MessageType_ROOM_CHAT {
		return
	}
	c.sendMessage(&protocol.Message{
		Type:    protocol.MessageType_ACK,
		To:      msg.From,
		Content: fmt.Sprintf("%d", msg.Timestamp),
	})
}

// Heartbeat
func (c *Client) Heartbeat() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.IsConnected() {
				return
			}
			if err := protocol.EncodeMessage(c.Conn, &protocol.Message{
				Type: protocol.MessageType_PING,
			}); err != nil {
				fmt.Println("\n>>>>>> 心跳发送失败:", err)
				c.signalDisconnect()
				return
			}
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

func (c *Client) signalDisconnect() {
	if c.connected.CompareAndSwap(true, false) {
		select {
		case c.disconnect <- struct{}{}:
		default:
		}
	}
}

// ReceiveMessage
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

		// PONG
		if msg.Type == protocol.MessageType_PONG {
			c.pongMu.Lock()
			c.lastPong = time.Now()
			c.pongMu.Unlock()
			continue
		}

		// Phase 4: 收到私聊/房间消息 → 发送 ACK
		c.sendAck(msg)

		switch msg.Type {
		case protocol.MessageType_SYSTEM:
			fmt.Println(msg.Content)

		case protocol.MessageType_CHAT:
			fmt.Printf("[%s]:%s\n", msg.From, msg.Content)

		case protocol.MessageType_PRIVATE:
			fmt.Printf("%s send to you: %s\n", msg.From, msg.Content)

		case protocol.MessageType_ROOM_CHAT:
			fmt.Printf("[Room]%s: %s\n", msg.From, msg.Content)

		default:
			fmt.Println(msg.Content)
		}
	}
}

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

// Reconnect
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

		if c.Conn != nil {
			c.Conn.Close()
		}
		c.Conn = conn
		c.connected.Store(true)

		c.pongMu.Lock()
		c.lastPong = time.Now()
		c.pongMu.Unlock()

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

// Menu
func (c *Client) Menu() bool {
	fmt.Println("\n1、群聊模式")
	fmt.Println("2、私聊模式")
	fmt.Println("3、更新用户名")
	fmt.Println("4、聊天室") // Phase 4
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
	case "4":
		c.flag = 4
		return true
	case "0":
		c.flag = 0
		return true
	default:
		fmt.Println(">>>>>>Invalid choice, please try again.")
		return false
	}
}

// GroupChat
func (c *Client) GroupChat() {
	fmt.Println(">>>>>>please input chat message (exit out)")
	for {
		chatMsg := c.readLine("")
		if chatMsg == "exit" {
			return
		}
		if chatMsg == "" {
			if !c.IsConnected() {
				return
			}
			continue
		}
		c.sendMessage(&protocol.Message{
			Type:    protocol.MessageType_CHAT,
			Content: chatMsg,
		})
	}
}

func (c *Client) OnlineUserList() {
	c.sendMessage(&protocol.Message{Type: protocol.MessageType_WHO})
}

// PrivateChat
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
				break
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

// UpdateName
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

// ---------- Phase 4: 聊天室功能 ----------

func (c *Client) RoomMenu() {
	fmt.Println("\n--- 聊天室菜单 ---")
	fmt.Println("1、创建聊天室")
	fmt.Println("2、加入聊天室")
	fmt.Println("3、离开聊天室")
	fmt.Println("4、查看聊天室列表")
	fmt.Println("5、进入聊天室发消息")
	fmt.Println("0、返回主菜单")

	input := c.readLine("")

	switch input {
	case "1":
		c.RoomCreate()
	case "2":
		c.RoomJoin()
	case "3":
		c.RoomLeave()
	case "4":
		c.RoomList()
	case "5":
		c.RoomChat()
	case "0":
		return
	default:
		fmt.Println(">>>>>>Invalid choice")
	}
}

func (c *Client) RoomCreate() {
	name := c.readLine(">>>>>>输入聊天室名称: ")
	if name == "" {
		return
	}
	c.sendMessage(&protocol.Message{
		Type:    protocol.MessageType_ROOM_CREATE,
		Content: name,
	})
}

func (c *Client) RoomJoin() {
	c.RoomList()
	name := c.readLine(">>>>>>输入要加入的聊天室名称: ")
	if name == "" {
		return
	}
	c.sendMessage(&protocol.Message{
		Type:    protocol.MessageType_ROOM_JOIN,
		Content: name,
	})
}

func (c *Client) RoomLeave() {
	name := c.readLine(">>>>>>输入要离开的聊天室名称: ")
	if name == "" {
		return
	}
	c.sendMessage(&protocol.Message{
		Type:    protocol.MessageType_ROOM_LEAVE,
		Content: name,
	})
}

func (c *Client) RoomList() {
	c.sendMessage(&protocol.Message{Type: protocol.MessageType_ROOM_LIST})
}

func (c *Client) RoomChat() {
	c.RoomList()
	roomName := c.readLine(">>>>>>输入聊天室名称: ")
	if roomName == "" {
		return
	}
	fmt.Printf(">>>>>>进入聊天室 '%s' (输入 exit 退出)\n", roomName)
	for {
		chatMsg := c.readLine("")
		if chatMsg == "exit" {
			return
		}
		if chatMsg == "" {
			if !c.IsConnected() {
				return
			}
			continue
		}
		c.sendMessage(&protocol.Message{
			Type:    protocol.MessageType_ROOM_CHAT,
			To:      roomName, // To 字段存放房间名
			Content: chatMsg,
		})
	}
}

// ---------- 主业务 ----------

func (c *Client) Run() {
	go c.Heartbeat()
	go c.ReceiveMessage()

	for c.flag != 0 {
		if !c.IsConnected() {
			fmt.Println("\n>>>>>> 连接已断开，开始重连...")
			if !c.reconnect() {
				fmt.Println(">>>>>> 重连失败，程序退出")
				return
			}
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
		case 4:
			c.RoomMenu() // Phase 4
		case 0:
			fmt.Println("退出")
		}
	}

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
