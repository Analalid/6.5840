package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// 测试在wsl2中os.Getuid得到的结果是否合法，tips：windows下得到的是-1
func main() {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getpid())
	for {
		fmt.Println(s)
		time.Sleep(2000)
	}
	//测试成功！结果/var/tmp/5840-mr-1000， 说明得到的UID是1000
}
