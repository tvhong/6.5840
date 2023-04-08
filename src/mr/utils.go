package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "fmt"

func printLn(args ...interface{}) {
	DEBUG := true
	if DEBUG {
		fmt.Println(args...)
	}
}
