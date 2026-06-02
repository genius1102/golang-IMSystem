package protocol

// Phase 3 新增消息类型（在 proto 枚举基础上扩展）
const (
	MessageType_PING MessageType = 5 // 心跳 ping
	MessageType_PONG MessageType = 6 // 心跳 pong
)

// Phase 4 新增消息类型
const (
	MessageType_ROOM_CREATE MessageType = 7  // 创建聊天室
	MessageType_ROOM_JOIN   MessageType = 8  // 加入聊天室
	MessageType_ROOM_LEAVE  MessageType = 9  // 离开聊天室
	MessageType_ROOM_LIST   MessageType = 10 // 查询聊天室列表
	MessageType_ROOM_CHAT   MessageType = 11 // 聊天室内发送消息
	MessageType_ACK         MessageType = 12 // 消息确认
)

// 扩展消息类型名称映射
func init() {
	MessageType_name[5] = "PING"
	MessageType_name[6] = "PONG"
	MessageType_name[7] = "ROOM_CREATE"
	MessageType_name[8] = "ROOM_JOIN"
	MessageType_name[9] = "ROOM_LEAVE"
	MessageType_name[10] = "ROOM_LIST"
	MessageType_name[11] = "ROOM_CHAT"
	MessageType_name[12] = "ACK"

	MessageType_value["PING"] = 5
	MessageType_value["PONG"] = 6
	MessageType_value["ROOM_CREATE"] = 7
	MessageType_value["ROOM_JOIN"] = 8
	MessageType_value["ROOM_LEAVE"] = 9
	MessageType_value["ROOM_LIST"] = 10
	MessageType_value["ROOM_CHAT"] = 11
	MessageType_value["ACK"] = 12
}
