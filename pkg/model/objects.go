package model

import (
	"crypto/sha256"
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/util"
)

type Source struct {
	Keys     map[string]string
	Location string
}

func (s Source) KeyHash() string {
	pairs := util.NewStringKVPairs(s.Keys)
	hash := sha256.New()
	pairs.WriteTo(hash)
	return fmt.Sprintf("%x", hash.Sum(nil))
}
