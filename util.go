package cluster

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// Global UID config
var cGUID struct {
	hostname string
	pid      int
	inc      uint32
	sync.Mutex
}

// Init GUID configuration
func init() {
	cGUID.hostname, _ = os.Hostname()
	cGUID.pid = os.Getpid()
	cGUID.inc = 0xffffffff
	if cGUID.hostname == "" {
		cGUID.hostname = "localhost"
	}
}

// Create a new GUID
func newGUID(prefix string) string {
	return newGUIDAt(prefix, time.Now())
}

// Create a new GUID for a certain time
func newGUIDAt(prefix string, at time.Time) string {
	cGUID.Lock()
	defer cGUID.Unlock()

	cGUID.inc++
	return fmt.Sprintf("%s-%s-%d-%d-%d", prefix, cGUID.hostname, cGUID.pid, at.Unix(), cGUID.inc)
}

type int32Slice []int32

func (s int32Slice) Len() int           { return len(s) }
func (s int32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s int32Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s int32Slice) Sorted() []int32    { sort.Sort(s); return []int32(s) }