package main

import (
	"flag"
	"fmt"
	"net"
)

type Client struct {
	ServerIP   string
	ServerPort int
	Name       string
	Conn       net.Conn
}

func NewClient(serverIp string, serverPort int) *Client {
	client := &Client{
		ServerIP:   serverIp,
		ServerPort: serverPort,
	}

	conn , err := net.Dial("tcp", fmt.Sprintf("%s:%d", serverIp, serverPort))
	if err != nil {
		fmt.Println("Dial server failed:", err)
		return nil
	}

	client.Conn = conn

	return client

}

var serverIp string
var serverPort int

func init() {
	flag.StringVar(&serverIp,"ip","127.0.0.1","server ip")
	flag.IntVar(&serverPort,"port",8888,"server port")
}


func main() {
	flag.Parse()
	client := NewClient(serverIp, serverPort)
	if client == nil {
		fmt.Println(">>>>>>>>failed connect to server......")
		return
	}

	fmt.Println(">>>>>>>>success connect to server......")

	//启动客户端服务
	select {}
}