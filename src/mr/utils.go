package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "fmt"

func printLn(args ...interface{}) {
	if isDebug() {
		fmt.Println(args...)
	}
}

func printf(s string, args ...interface{}) {
	if isDebug() {
		fmt.Printf(s, args...)
	}
}

func isDebug() bool {
	return true
}
