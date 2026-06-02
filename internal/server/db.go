package server

import (
	"database/sql"
	"fmt"
	"imsystem/internal/protocol"

	_ "modernc.org/sqlite"
)

const dbDriver = "sqlite"

// InitDB 初始化 SQLite 数据库，创建消息表
func InitDB(path string) (*sql.DB, error) {
	db, err := sql.Open(dbDriver, path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	// 连接池配置（SQLite 单写，连接数不宜过大）
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// 创建消息表
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			type      INTEGER NOT NULL DEFAULT 0,
			from_user TEXT    NOT NULL DEFAULT '',
			to_user   TEXT    NOT NULL DEFAULT '',
			content   TEXT    NOT NULL DEFAULT '',
			timestamp INTEGER NOT NULL DEFAULT 0,
			delivered INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_undelivered
			ON messages(to_user, delivered);
	`)
	if err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	return db, nil
}

// SaveMessage 将消息持久化到数据库
func SaveMessage(db *sql.DB, msg *protocol.Message) error {
	_, err := db.Exec(
		`INSERT INTO messages (type, from_user, to_user, content, timestamp, delivered)
		 VALUES (?, ?, ?, ?, ?, 0)`,
		int32(msg.Type), msg.From, msg.To, msg.Content, msg.Timestamp,
	)
	return err
}

// GetUndeliveredMessages 获取某用户的未投递离线消息
func GetUndeliveredMessages(db *sql.DB, username string) ([]*protocol.Message, error) {
	rows, err := db.Query(
		`SELECT id, type, from_user, to_user, content, timestamp
		 FROM messages
		 WHERE to_user = ? AND delivered = 0
		 ORDER BY id`,
		username,
	)
	if err != nil {
		return nil, fmt.Errorf("query undelivered: %w", err)
	}
	defer rows.Close()

	var msgs []*protocol.Message
	for rows.Next() {
		var id int64
		var msgType int32
		msg := &protocol.Message{}
		if err := rows.Scan(&id, &msgType, &msg.From, &msg.To, &msg.Content, &msg.Timestamp); err != nil {
			continue
		}
		msg.Type = protocol.MessageType(msgType)
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// MarkMessagesDelivered 将某用户的离线消息标记为已投递
func MarkMessagesDelivered(db *sql.DB, username string) error {
	_, err := db.Exec(
		`UPDATE messages SET delivered = 1
		 WHERE to_user = ? AND delivered = 0`,
		username,
	)
	return err
}

// Phase 4: AckMessage 通过 ACK 确认单条消息已投递
// toUser 是接收者，fromUser 是原始发送者，ts 是原始消息时间戳
func AckMessage(db *sql.DB, toUser, fromUser, ts string) error {
	result, err := db.Exec(
		`UPDATE messages SET delivered = 1
		 WHERE to_user = ? AND from_user = ? AND CAST(timestamp AS TEXT) = ? AND delivered = 0`,
		toUser, fromUser, ts,
	)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n > 0 {
		fmt.Printf("message ack'd: to=%s from=%s ts=%s\n", toUser, fromUser, ts)
	}
	return nil
}
