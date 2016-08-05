package store

import (
	"strings"
)

// Normalize the key for each store to the form:
//
//     /path/to/key
//
func Normalize(key string) string {
	return "/" + join(SplitKey(key))
}

// GetDirectory gets the full directory part of
// the key to the form:
//
//     /path/to/
//
func GetDirectory(key string) string {
	parts := SplitKey(key)
	parts = parts[:len(parts)-1]
	return "/" + join(parts)
}

// SplitKey splits the key to extract path informations
func SplitKey(key string) (path []string) {
	if strings.Contains(key, "/") {
		path = strings.Split(key, "/")
	} else {
		path = []string{key}
	}
	return path
}

// join the path parts with '/'
func join(parts []string) string {
	return strings.Join(parts, "/")
}
