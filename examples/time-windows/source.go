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
	PartitionFrequency time.Duration
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
	s.PartitionFrequency = pf
	s.SourceFrequency = sf
	return
}

func (spec Spec) Values(t time.Time) (partkey string, sourceval string) {
	pt := t.Truncate(spec.PartitionFrequency).Format(time.RFC3339)
	st := t.Truncate(spec.SourceFrequency).Format(time.RFC3339)
	return fmt.Sprintf("part-%s", pt), st
}

func (spec Spec) Push(partition string, sourceval string) error {
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
		fmt.Sprintf("http://%s/partitions/%s/%s/%s", spec.Endpoint, spec.Domain, partition, spec.Token),
		"application/json",
		buff,
	)

	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Server responded with unexpected status: %s", response.Status)
	}

	fmt.Printf("Domain %q partition %q token %q source val %q: %s\n", spec.Domain, partition, spec.Token, sourceval, response.Status)
	return nil
}

func main() {
	spec, err := SpecFromArgs(os.Args[1:])

	if err != nil {
		panic(err)
	}

	fmt.Printf("Pusher for %s domain %s token %s with partition freq %s and source freq %q\n", spec.Endpoint, spec.Domain, spec.Token, spec.PartitionFrequency, spec.SourceFrequency)

	var partition string
	var sourceval string

	errs := 0

	for errs < 3 {
		pk, sv := spec.Values(time.Now())
		if pk != partition || sourceval != sv {
			// time to emit a new source
			err := spec.Push(pk, sv)
			if err != nil {
				fmt.Println("Error pushing:", err)
				errs++
			} else {
				partition = pk
				sourceval = sv
			}
		} else {
			// sleep for a second.
			time.Sleep(time.Second)
		}
	}
}
