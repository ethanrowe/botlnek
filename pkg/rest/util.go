package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/model"
	"net/http"
)

func JsonBodyDecoder(r *http.Request) (*json.Decoder, error) {
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		return nil, fmt.Errorf("Invalid content-type %q", ct)
	}
	if r.Body == nil {
		return nil, errors.New("No JSON body provided")
	}
	return json.NewDecoder(r.Body), nil
}

func DomainFromRequest(r *http.Request) (m model.Domain, e error) {
	decoder, e := JsonBodyDecoder(r)
	if e != nil {
		return
	}
	e = decoder.Decode(&m)
	if e == nil && decoder.More() {
		e = errors.New("Only one object can be provided in the body")
	}
	return
}
