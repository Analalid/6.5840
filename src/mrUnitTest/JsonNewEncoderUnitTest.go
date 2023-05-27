package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	dir, _ := os.Getwd()
	fmt.Println(dir)
	file, err := os.OpenFile(dir+"/main/test.json", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(err)
	}
	enc := json.NewEncoder(file)
	people := []Person{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 28},
		{Name: "Charlie", Age: 35},
	}
	for _, p := range people {
		if err := enc.Encode(&p); err != nil {
			fmt.Println("写入文件失败：", err)
		}
	}
	err = file.Close()
	if err != nil {
		fmt.Println(err)
	}
}
