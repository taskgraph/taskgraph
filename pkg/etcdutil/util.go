package etcdutil

import "strings"

func IsKeyNotFound(err error) bool {
	if strings.Contains(err.Error(), "Key not found") {
		return true
	}
	return false
}
