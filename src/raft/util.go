package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dInfo  logTopic = "INFO"
	dWarn  logTopic = "WARN"
	dError logTopic = "ERRO"

	dLog  logTopic = "LOG1"
	dLog2 logTopic = "LOG2"

	dRpc    logTopic = "RPC_"
	dTimer  logTopic = "TIMR"
	dVote   logTopic = "VOTE"
	dState  logTopic = "STAT" // state change
	dClient logTopic = "CLNT" // client interactions
)

var debugStart time.Time
var debugVerbosity int

func Debug(server int, currentTerm int, topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		writeLog(log.Printf, server, currentTerm, topic, format, a)
	}
}

func Fatal(server int, currentTerm int, format string, a ...interface{}) {
	writeLog(log.Fatalf, server, currentTerm, dError, format, a)
}

func writeLog(
	logFn func(format string, a ...interface{}),
	server int,
	currentTerm int,
	topic logTopic,
	format string,
	a ...interface{}) {
	time := time.Since(debugStart).Microseconds()
	time /= 100
	prefix := fmt.Sprintf("%06d %v S%v t%v ", time, string(topic), server, currentTerm)
	format = prefix + format
	logFn(format, a...)
}

func Random(min int, max int) int {
	if max < min {
		min, max = max, min
	}

	return rand.Intn(max-min+1) + min
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	rand.Seed(time.Now().UnixNano())
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
