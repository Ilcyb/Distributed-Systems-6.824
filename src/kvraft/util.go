package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func getVerbosity() int {
	v := os.Getenv("KVVERBOSE")
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

var debugStart time.Time
var debugVerbosity int

type logTopic string

const (
	dClark    logTopic = "CLAK"
	dKVClient logTopic = "KVCL"
	dKVServer logTopic = "KVSV"
)

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
