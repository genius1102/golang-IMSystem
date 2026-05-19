package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

type Client struct {
	ServerIP   string
	ServerPort int
	Name       string
	Conn       net.Conn
	flag       int
}

func NewClient(serverIp string, serverPort int) *Client {
	client := &Client{
		ServerIP:   serverIp,
		ServerPort: serverPort,
		flag:       999,
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", serverIp, serverPort))
	if err != nil {
		fmt.Println("Dial server failed:", err)
		return nil
	}

	client.Conn = conn

	return client

}

// 客户端接收服务器消息
func (client *Client) ReceiveMessage() {
	// 接收服务器消息，打印标准输出到stdout
	io.Copy(os.Stdout, client.Conn)
}

// 显示菜单
func (client *Client) Menu() bool {
	fmt.Println("1、群聊模式")
	fmt.Println("2、私聊模式")
	fmt.Println("3、更新用户名")
	fmt.Println("0、退出")

	fmt.Println("Please choose mod(1/2/3/0)")
	fmt.Scanln(&client.flag)

	if client.flag >= 0 && client.flag <= 3 {
		return true
	} else {
		fmt.Println("Invalid choice, please try again.")
		return false
	}
}

// 更新用户名
func (client *Client) UpdateName() bool {
	fmt.Println("Please input rename: ")
	fmt.Scanln(&client.Name)
	sendMsg := "rename|" + client.Name + "\n"
	_, err := client.Conn.Write([]byte(sendMsg))
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

		// 处理用户选择的业务
		switch client.flag {
		case 1:
			// 群聊模式
			fmt.Println("群聊模式")
			break
		case 2:
			// 私聊模式
			fmt.Println("私聊模式")
			break
		case 3:
			// 更新用户名
			client.UpdateName()
			break
		case 0:
			// 退出
			fmt.Println("退出")
			break
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

	//启动客户端服务
	client.Run()
}
