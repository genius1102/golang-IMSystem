package protocol

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

// EncodeMessage 将 Message 序列化，加上 4 字节长度前缀，写入 writer
//
//	帧格式: [4字节大端长度][Protobuf序列化数据]
//	这样接收方先读 4 字节知道消息多长，再读完整消息体
//	消息内容可以包含任意字符（\n、|、中文、甚至二进制），不会被误解析
func EncodeMessage(w io.Writer, msg *Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("proto marshal: %w", err)
	}

	// 写 4 字节长度前缀（大端序）
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}

	// 写消息体
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write body: %w", err)
	}

	return nil
}

// DecodeMessage 从 reader 读取一条长度前缀帧，反序列化为 Message
//
//	1. 读 4 字节 → 得到消息长度 N
//	2. 读 N 字节 → 得到 Protobuf 数据
//	3. 反序列化
func DecodeMessage(r io.Reader) (*Message, error) {
	// 读长度前缀
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)

	// 防止恶意超大消息（1MB 上限）
	if length > 1024*1024 {
		return nil, fmt.Errorf("message too large: %d bytes", length)
	}

	// 读消息体
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	// 反序列化
	msg := &Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("proto unmarshal: %w", err)
	}

	return msg, nil
}
