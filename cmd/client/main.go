package main

import (
	"bufio"
	"flag"
	"fmt"
	"imsystem/internal/protocol"
	"io"
	"net"
	"os"
	"strings"
)

type Client struct {
	ServerIP   string
	ServerPort int
	Name       string
	Conn       net.Conn
	flag       int
	reader     *bufio.Reader // 复用输入读取器
}

func NewClient(serverIp string, serverPort int) *Client {
	client := &Client{
		ServerIP:   serverIp,
		ServerPort: serverPort,
		flag:       999,
		reader:     bufio.NewReader(os.Stdin),
	}

	conn, err := net.Dial("tcp", net.JoinHostPort(serverIp, fmt.Sprintf("%d", serverPort)))
	if err != nil {
		fmt.Println("Dial server failed:", err)
		return nil
	}

	client.Conn = conn
	return client
}

// 读取一行用户输入（支持空格，去掉末尾换行）
func (c *Client) readLine(prompt string) string {
	fmt.Print(prompt)
	input, err := c.reader.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSpace(input)
}

// 客户端接收服务器消息，并检测断线
func (client *Client) ReceiveMessage() {
	_, err := io.Copy(os.Stdout, client.Conn)
	if err != nil {
		fmt.Println("\n>>>>>>与服务器的连接已断开:", err)
	}
	fmt.Println("\n>>>>>>按 Enter 退出...")
	os.Exit(0)
}

// 显示菜单
func (client *Client) Menu() bool {
	fmt.Println("1、群聊模式")
	fmt.Println("2、私聊模式")
	fmt.Println("3、更新用户名")
	fmt.Println("0、退出")

	input := client.readLine("")

	switch input {
	case "1":
		client.flag = 1
		return true
	case "2":
		client.flag = 2
		return true
	case "3":
		client.flag = 3
		return true
	case "0":
		client.flag = 0
		return true
	default:
		fmt.Println(">>>>>>Invalid choice, please try again.")
		return false
	}
}

// 群聊模式
func (client *Client) GroupChat() {
	fmt.Println(">>>>>>please input chat message (exit out)")

	for {
		chatMsg := client.readLine("")
		if chatMsg == "exit" || chatMsg == "" {
			return
		}

		_, err := client.Conn.Write([]byte(chatMsg + "\n"))
		if err != nil {
			fmt.Println("conn.Write err:", err)
			return
		}
	}
}

// 在线用户列表
func (client *Client) OnlineUserList() {
	_, err := client.Conn.Write([]byte(protocol.CmdWho + "\n"))
	if err != nil {
		fmt.Println("conn.Write err:", err)
	}
}

// 私聊模式
func (client *Client) PrivateChat() {
	client.OnlineUserList()

	remoteName := client.readLine(">>>>>>请输入聊天对象用户名（输入exit退出）: ")
	if remoteName == "exit" || remoteName == "" {
		return
	}

	for {
		chatMsg := client.readLine(">>>>>>请输入消息内容（输入exit退出）：")
		if chatMsg == "exit" || chatMsg == "" {
			break
		}

		msg := protocol.BuildPrivateMsg(remoteName, chatMsg)
		_, err := client.Conn.Write([]byte(msg + "\n"))
		if err != nil {
			fmt.Println("conn.Write err:", err)
			return
		}
	}
}

// 更新用户名
func (client *Client) UpdateName() bool {
	newName := client.readLine(">>>>>>Please input rename: ")
	if newName == "" {
		return false
	}

	client.Name = newName
	msg := protocol.BuildRenameMsg(newName)
	_, err := client.Conn.Write([]byte(msg + "\n"))
	if err != nil {
		fmt.Println("conn.Write err:", err)
		return false
	}
	return true
}

// 主业务
func (client *Client) Run() {
	for client.flag != 0 {
		for client.Menu() != true {
		}

		switch client.flag {
		case 1:
			client.GroupChat()
		case 2:
			client.PrivateChat()
		case 3:
			client.UpdateName()
		case 0:
			fmt.Println("退出")
		}
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

	// 启动接收服务器消息的goroutine
	go client.ReceiveMessage()

	fmt.Println(">>>>>>>>success connect to server......")

	// 启动客户端业务
	client.Run()
}
