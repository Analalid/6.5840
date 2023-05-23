package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
)

// 测试查询文件夹下所有文件的Demo
func main() {
	//Current directory: /home/wsb/6.5840/src/main
	dir, err := os.Getwd()
	dir = dir + "/main"
	files, err := ioutil.ReadDir(dir)
	//fmt.Println("查询的文件夹是 " + dir)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	var result []string
	//匹配pg开头，txt结尾
	pattern := "^pg.*txt$"
	regex, err := regexp.Compile(pattern)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	for _, file := range files {
		if !file.IsDir() && regex.MatchString(file.Name()) {
			result = append(result, filepath.Join(dir, file.Name()))
			fmt.Println(filepath.Join(dir, file.Name()))
		}
	}

	fmt.Printf("Result: %v\n", result)
}
