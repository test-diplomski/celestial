package etcd

import "strings"

const (
	Tombstone = "!!Tombstone"
)

func firstN(text string, n int) string {
	return text[:n]
}

func lastN(text string, n int) string {
	return text[len(text)-n:]
}

func merge(text string, n int) string {
	return strings.Join([]string{firstN(text, n), "....", lastN(text, n)}, "")
}
