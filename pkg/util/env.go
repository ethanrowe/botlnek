package util

import (
	"os"
)

func Getenv(envvar, defaultval string) string {
	val := os.Getenv(envvar)
	if val == "" {
		return defaultval
	}
	return val
}
