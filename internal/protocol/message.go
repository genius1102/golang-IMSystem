package protocol

import "strings"

// 命令常量
const (
	CmdWho    = "who"
	CmdRename = "rename|"
	CmdTo     = "to|"
)

// 消息类型
const (
	TypeSystem  = "system"
	TypeChat    = "chat"
	TypePrivate = "private"
)

// 构建系统广播消息（上线/下线/群聊）
// 格式: [addr]name:content
func BuildSystemMsg(addr, name, action string) string {
	return "[" + addr + "]" + name + action
}

// 构建私聊消息
// 格式: to|targetName|content
func BuildPrivateMsg(target, content string) string {
	return "to|" + target + "|" + content
}

// 构建改名消息
// 格式: rename|newName
func BuildRenameMsg(newName string) string {
	return "rename|" + newName
}

// 解析私聊消息: "to|targetName|content"
// 返回: target, content, ok
func ParsePrivateMsg(msg string) (target, content string, ok bool) {
	if len(msg) < 4 || msg[:3] != CmdTo {
		return "", "", false
	}

	// 去掉 "to|" 前缀
	payload := msg[3:]

	// 找第一个 | 分隔 target 和 content
	idx := strings.Index(payload, "|")
	if idx == -1 {
		return "", "", false
	}

	target = payload[:idx]
	content = payload[idx+1:]

	if target == "" || content == "" {
		return "", "", false
	}

	return target, content, true
}

// 解析改名消息: "rename|newName"
func ParseRenameMsg(msg string) (newName string, ok bool) {
	if len(msg) < 8 || msg[:7] != CmdRename {
		return "", false
	}
	newName = msg[7:]
	if newName == "" {
		return "", false
	}
	return newName, true
}

// 判断是否为查询在线用户命令
func IsWhoCmd(msg string) bool {
	return msg == CmdWho
}
