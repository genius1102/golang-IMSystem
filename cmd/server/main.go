package main

import (
	"flag"
	"fmt"
	"imsystem/internal/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	dbPath := flag.String("db", "./imserver.db", "SQLite database path")
	flag.Parse()

	// 初始化 SQLite 数据库
	db, err := server.InitDB(*dbPath)
	if err != nil {
		fmt.Println("init db failed:", err)
		os.Exit(1)
	}
	fmt.Printf("database initialized: %s\n", *dbPath)

	s := server.NewServer("127.0.0.1", 8888, db)

	// 监听系统信号，实现优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		s.Stop()
		os.Exit(0)
	}()

	if err := s.Start(); err != nil {
		panic(err)
	}
}
