package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type Spec struct {
	Endpoint           string
	Domain             string
	AggregateFrequency time.Duration
	Token              string
	SourceFrequency    time.Duration
}

func SpecFromArgs(args []string) (s Spec, e error) {
	pf, e := time.ParseDuration(args[3])
	if e != nil {
		return
	}
	sf, e := time.ParseDuration(args[4])
	if e != nil {
		return
	}
	s.Endpoint = args[0]
	s.Domain = args[1]
	s.Token = args[2]
	s.AggregateFrequency = pf
	s.SourceFrequency = sf
	return
}

func (spec Spec) Values(t time.Time) (partkey string, sourceval string) {
	pt := t.Truncate(spec.AggregateFrequency).Format(time.RFC3339)
	st := t.Truncate(spec.SourceFrequency).Format(time.RFC3339)
	return fmt.Sprintf("part-%s", pt), st
}

func (spec Spec) Push(aggregate string, sourceval string) error {
	source := map[string]map[string]string{
		"Keys": map[string]string{
			"push-key": sourceval,
		},
		"Attrs": map[string]string{
			sourceval: "hi.",
		},
	}
	body, err := json.Marshal(source)
	if err != nil {
		return err
	}
	buff := bytes.NewReader(body)
	response, err := http.DefaultClient.Post(
		fmt.Sprintf("http://%s/aggregates/%s/%s/%s", spec.Endpoint, spec.Domain, aggregate, spec.Token),
		"application/json",
		buff,
	)

	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Server responded with unexpected status: %s", response.Status)
	}

	fmt.Printf("Domain %q aggregate %q token %q source val %q: %s\n", spec.Domain, aggregate, spec.Token, sourceval, response.Status)
	return nil
}

func main() {
	spec, err := SpecFromArgs(os.Args[1:])

	if err != nil {
		panic(err)
	}

	fmt.Printf("Pusher for %s domain %s token %s with aggregate freq %s and source freq %q\n", spec.Endpoint, spec.Domain, spec.Token, spec.AggregateFrequency, spec.SourceFrequency)

	var aggregate string
	var sourceval string

	errs := 0

	for errs < 3 {
		pk, sv := spec.Values(time.Now())
		if pk != aggregate || sourceval != sv {
			// time to emit a new source
			err := spec.Push(pk, sv)
			if err != nil {
				fmt.Println("Error pushing:", err)
				errs++
			} else {
				aggregate = pk
				sourceval = sv
			}
		} else {
			// sleep for a second.
			time.Sleep(time.Second)
		}
	}
}
