package main

import (
	"fmt"
	"log"
	"time"
)

func check(msg string, args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		log.Println(fmt.Sprintf("fatal error: %s", msg))
		log.Fatal(err)
	}
}

func measure(name string) func() {
	start := time.Now()
	return func() {
		log.Printf("%s took %v\n", name, time.Since(start))
	}
}
