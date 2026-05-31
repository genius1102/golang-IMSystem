package main

import (
	"fmt"
	"imsystem/internal/protocol"
	"net"
	"time"
)

// drainMsg 读取并消费一条消息
func drainMsg(conn net.Conn) *protocol.Message {
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	msg, err := protocol.DecodeMessage(conn)
	if err != nil {
		return &protocol.Message{Content: fmt.Sprintf("ERR: %v", err)}
	}
	return msg
}

func main() {
	fmt.Println("=== Phase 3 集成测试 ===\n")

	// ============ 测试 1: 心跳 PING/PONG ============
	fmt.Println("--- 测试 1: 心跳 PING/PONG ---")
	conn1, _ := net.Dial("tcp", "127.0.0.1:8888")
	defer conn1.Close()

	// 消费初始上线广播
	initMsg := drainMsg(conn1)
	fmt.Printf("  初始消息: [%v] %s\n", initMsg.Type, initMsg.Content)

	// 发送 PING
	protocol.EncodeMessage(conn1, &protocol.Message{Type: protocol.MessageType_PING})
	start := time.Now()
	resp := drainMsg(conn1)
	if resp != nil && resp.Type == protocol.MessageType_PONG {
		fmt.Printf("  PASS: PING → PONG 响应 (耗时 %v)\n\n", time.Since(start))
	} else {
		fmt.Printf("  FAIL: 期望 PONG, 得到 [%v] %s\n\n", resp.Type, resp.Content)
	}

	// ============ 测试 2: 改名 ============
	fmt.Println("--- 测试 2: 用户名修改 ---")
	protocol.EncodeMessage(conn1, &protocol.Message{
		Type: protocol.MessageType_RENAME, Content: "Alice",
	})
	msg := drainMsg(conn1)
	fmt.Printf("  conn1 改名响应: %s\n", msg.Content)

	// ============ 测试 3: 第二个客户端连接 ============
	fmt.Println("\n--- 测试 3: 第二个客户端 ---")
	conn2, _ := net.Dial("tcp", "127.0.0.1:8888")
	defer conn2.Close()

	// conn2 初始消息
	initMsg2 := drainMsg(conn2)
	fmt.Printf("  conn2 初始消息: [%v] %s\n", initMsg2.Type, initMsg2.Content)

	// conn1 也会收到 conn2 的上线广播
	onlineMsg := drainMsg(conn1)
	fmt.Printf("  conn1 收到上线广播: %s\n", onlineMsg.Content)

	// conn2 改名为 Bob
	protocol.EncodeMessage(conn2, &protocol.Message{
		Type: protocol.MessageType_RENAME, Content: "Bob",
	})
	msg2 := drainMsg(conn2)
	fmt.Printf("  conn2 改名响应: %s\n", msg2.Content)

	// ============ 测试 4: 群聊 ============
	fmt.Println("\n--- 测试 4: 群聊消息 ---")
	protocol.EncodeMessage(conn1, &protocol.Message{
		Type: protocol.MessageType_CHAT, Content: "Hello everyone!",
	})
	// conn1 自己也收到广播
	chat1 := drainMsg(conn1)
	fmt.Printf("  conn1 收到: [%v] %s: %s\n", chat1.Type, chat1.From, chat1.Content)
	// conn2 也收到
	chat2 := drainMsg(conn2)
	fmt.Printf("  conn2 收到: [%v] %s: %s\n", chat2.Type, chat2.From, chat2.Content)

	// ============ 测试 5: 私聊 ============
	fmt.Println("\n--- 测试 5: 私聊消息 ---")
	protocol.EncodeMessage(conn1, &protocol.Message{
		Type: protocol.MessageType_PRIVATE, To: "Bob", Content: "Hello Bob!",
	})
	privMsg := drainMsg(conn2)
	fmt.Printf("  conn2 收到私聊: [%v] from=%s content=%s\n", privMsg.Type, privMsg.From, privMsg.Content)

	// ============ 测试 6: 离线消息 ============
	fmt.Println("\n--- 测试 6: 离线消息持久化 ---")
	// Bob(conn2) 断开
	conn2.Close()
	time.Sleep(300 * time.Millisecond)

	// 消费 conn1 收到的 "Bob 下线!" 广播
	offlineBroadcast := drainMsg(conn1)
	fmt.Printf("  conn1 收到: %s\n", offlineBroadcast.Content)

	// Alice 给 Bob 发私聊（Bob 离线）
	protocol.EncodeMessage(conn1, &protocol.Message{
		Type: protocol.MessageType_PRIVATE, To: "Bob", Content: "离线消息 #1: 你在吗?",
	})
	offResp1 := drainMsg(conn1)
	fmt.Printf("  conn1 发送离线消息 #1: %s\n", offResp1.Content)

	protocol.EncodeMessage(conn1, &protocol.Message{
		Type: protocol.MessageType_PRIVATE, To: "Bob", Content: "离线消息 #2: 看到回我",
	})
	offResp2 := drainMsg(conn1)
	fmt.Printf("  conn1 发送离线消息 #2: %s\n", offResp2.Content)

	// Bob 重新上线
	fmt.Println("\n  Bob 重新连接...")
	conn2b, _ := net.Dial("tcp", "127.0.0.1:8888")
	defer conn2b.Close()

	// Bob 初始上线消息
	bobInit := drainMsg(conn2b)
	fmt.Printf("  Bob 初始消息: %s\n", bobInit.Content)

	// conn1 收到 Bob 的上线广播
	bobOnline := drainMsg(conn1)
	fmt.Printf("  conn1 收到: %s\n", bobOnline.Content)

	// Bob 改名为 Bob（用回旧名）
	protocol.EncodeMessage(conn2b, &protocol.Message{
		Type: protocol.MessageType_RENAME, Content: "Bob",
	})
	bobRename := drainMsg(conn2b)
	fmt.Printf("  Bob 改名响应: %s\n", bobRename.Content)

	// Bob 应该收到离线消息推送
	fmt.Println("\n  Bob 收到的离线消息:")
	for i := 0; i < 10; i++ {
		conn2b.SetReadDeadline(time.Now().Add(2 * time.Second))
		msg, err := protocol.DecodeMessage(conn2b)
		if err != nil {
			break
		}
		fmt.Printf("  → [%v] from=%s content=%s\n", msg.Type, msg.From, msg.Content)
		if msg.Type == protocol.MessageType_PRIVATE && msg.From == "Alice" {
			fmt.Printf("     ✅ 离线消息投递成功!\n")
		}
	}

	// ============ 测试 7: WHO 查询 ============
	fmt.Println("\n--- 测试 7: 在线用户查询 ---")
	protocol.EncodeMessage(conn1, &protocol.Message{Type: protocol.MessageType_WHO})
	whoResp := drainMsg(conn1)
	fmt.Printf("  在线用户:\n%s\n", whoResp.Content)

	fmt.Println("\n=== 全部测试完成 ===")
}
