package store

import (
	"strings"
)

// CreateEndpoints creates a list of endpoints given the right scheme
func CreateEndpoints(addrs []string, scheme string) (entries []string) {
	for _, addr := range addrs {
		if !strings.Contains(addr, "://") {
			entries = append(entries, scheme+"://"+addr)
		} else {
			entries = append(entries, addr)
		}
	}
	return entries
}

// Normalize the key for each store to the form:
//
//     /path/to/key
//
func Normalize(key string) string {
	stringList := SplitKey(key)
	var parts []string
	for _, s := range stringList {
		if len(s) > 0 {
			parts = append(parts, s)
		}
	}

	if len(parts) >= 0 {
		return "/" + join(parts)
	}
	return "/"
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
