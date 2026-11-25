package analytics

import (
	"regexp"
	"sync"
)

var reCache sync.Map

func matched(re, s string) bool {
	v, ok := reCache.Load(re)
	if !ok {
		c, err := regexp.Compile(re)
		if err != nil {
			return false
		}
		v, _ = reCache.LoadOrStore(re, c)
	}
	return v.(*regexp.Regexp).MatchString(s)
}
