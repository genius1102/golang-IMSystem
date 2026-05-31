package protocol

// Phase 3 新增消息类型（在 proto 枚举基础上扩展）
const (
	MessageType_PING MessageType = 5 // 心跳 ping
	MessageType_PONG MessageType = 6 // 心跳 pong
)

// 扩展消息类型名称映射
func init() {
	MessageType_name[5] = "PING"
	MessageType_name[6] = "PONG"
	MessageType_value["PING"] = 5
	MessageType_value["PONG"] = 6
}
