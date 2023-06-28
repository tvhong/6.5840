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

	dTimer logTopic = "TIMR"
	dVote  logTopic = "VOTE"
	dState logTopic = "STAT"
	dRpc   logTopic = "RPC_"
)

var debugStart time.Time
var debugVerbosity int

func Debug(serverId int, topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v S%v ", time, string(topic), serverId)
		format = prefix + format
		log.Printf(format, a...)
	}
}

func Random(min int, max int) int {
	if max < min {
		min, max = max, min
	}

	return rand.Intn(max-min+1) + min
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
