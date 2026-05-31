package main

import (
	"imsystem/internal/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	s := server.NewServer("127.0.0.1", 8888)

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
