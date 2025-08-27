package o

import (
	"reflect"
	"strings"
	"time"
)

func InArray(item interface{}, array interface{}) bool {
	if harr, ok := ToSlice(array); ok {
		for _, _item := range harr {
			if _item == item {
				return true
			}
		}
	}
	return false
}

func InArrayStr(item string, array []string) bool {
	for _, v := range array {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
}

func ToSlice(arr interface{}) ([]interface{}, bool) {
	v := reflect.ValueOf(arr)
	if v.Kind() != reflect.Slice {
		return nil, false
	}
	l := v.Len()
	ret := make([]interface{}, l)
	for i := 0; i < l; i++ {
		ret[i] = v.Index(i).Interface()
	}
	return ret, true
}

func SafeGuardTask(fn func(), panic_sleep time.Duration) {
	wrap_fn := func() {
		defer func() {
			if err := recover(); nil != err {
				LogW("SafeGuardTask panic : %v", err)
			}
		}()
		fn()
	}

	for {
		wrap_fn()
		time.Sleep(panic_sleep)
	}
}
