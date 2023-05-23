package main

import (
	"fmt"
	"os"
)

// 测试当前进程所在目录的UnitDemo
func main() {
	dir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("Current directory: %s\n", dir)
}
