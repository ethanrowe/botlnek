package rest

import (
	"errors"
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/model"
	"net/http"
	"strings"
)

func HelloWorldRoute(r *http.Request) (JsonResponder, error) {
	result := struct {
		HttpMethod string
		RemoteAddr string
		Path       string
	}{
		r.Method,
		r.RemoteAddr,
		r.URL.Path,
	}
	return NewJsonResponse(200, result), nil
}

type RestApplication struct {
	DomainWriter    model.DomainWriter
	DomainReader    model.DomainReader
	PartitionWriter model.PartitionWriter
	PartitionReader model.PartitionReader
}

func (app *RestApplication) DomainsCollectionRoute(r *http.Request) (JsonResponder, error) {
	if r.Method == http.MethodGet {
		// For now we'll always respond with an empty list.
		return NewJsonResponse(200, struct{ Domains []string }{make([]string, 0)}), nil
	}
	if r.Method == http.MethodPost {
		domain, err := DomainFromRequest(r)
		if err != nil {
			return NewJsonErrorResponse(http.StatusBadRequest, err), nil
		}

		result, err := app.DomainWriter.AppendNewDomain(domain)
		if err != nil {
			return nil, err
		}

		var statusCode int
		if result == nil {
			statusCode = http.StatusAccepted
		} else {
			statusCode = http.StatusCreated
		}
		// Need to add a location header to the response,
		// but I'll figure out the reverse routing later.
		return NewJsonResponse(statusCode, nil), nil
	}
	return NewJsonErrorResponse(http.StatusMethodNotAllowed, errors.New("Only GET/POST are supported")), nil
}

func (app *RestApplication) DomainRoute(r *http.Request) (JsonResponder, error) {
	if r.Method == http.MethodGet {
		key := model.DomainKey(strings.TrimPrefix(r.URL.Path, "/domains/"))
		domain, err := app.DomainReader.GetDomain(key)
		if err != nil {
			return nil, err
		}
		if domain == nil {
			return NewJsonErrorResponse(http.StatusNotFound, errors.New("Cannot find domain: "+string(key))), nil
		}
		return NewJsonResponse(200, domain), nil
	}
	return NewJsonErrorResponse(http.StatusMethodNotAllowed, errors.New("Only GET is supported")), nil
}

func (app *RestApplication) PartitionsRoute(r *http.Request) (JsonResponder, error) {
	keys := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/partitions/"), "/", 3)
	fmt.Printf("PartitionsRoute path %q: %q\n", r.URL.Path, keys)

	switch r.Method {
	case http.MethodGet:
		if len(keys) == 2 {
			p, e := app.PartitionReader.GetPartition(model.DomainKey(keys[0]), model.PartitionKey(keys[1]))
			if p != nil {
				return NewJsonResponse(http.StatusOK, p), e
			}
		}
		return NewJsonResponse(http.StatusNotFound, nil), nil
	case http.MethodPost:
		var e error
		fmt.Println("POST case")
		if len(keys) == 3 {
			fmt.Println("Parsing source from body")
			s, e := SourceFromRequest(r)
			if e == nil {
				fmt.Printf("Appending source: %q\n", s)
				resp, e := app.PartitionWriter.AppendNewSource(
					model.DomainKey(keys[0]),
					model.PartitionKey(keys[1]),
					keys[2],
					s,
				)
				if e != nil {
					fmt.Println("Error during append")
					return nil, e
				}
				if resp == nil {
					return NewJsonResponse(http.StatusAccepted, nil), nil
				}
				return NewJsonResponse(http.StatusCreated, nil), nil
			}
			fmt.Println("Error parsing:", e)
			return nil, e
		}
		return NewJsonErrorResponse(http.StatusNotFound, nil), e
	default:
		return NewJsonErrorResponse(http.StatusMethodNotAllowed, errors.New("Only GET and POST supported")), nil
	}
}

func HandleJsonRoute(mux *http.ServeMux, pattern string, h func(*http.Request) (JsonResponder, error)) {
	mux.Handle(pattern, NewJsonHandler(h))
}

func (app *RestApplication) ApplyRoutes(mux *http.ServeMux) {
	HandleJsonRoute(mux, "/", HelloWorldRoute)
	// Get a list of domains;
	// Post a new domain
	HandleJsonRoute(mux, "/domains", app.DomainsCollectionRoute)
	// Get a specific domain
	HandleJsonRoute(mux, "/domains/", app.DomainRoute)
	// Post a new partition
	// Get an existing partition
	HandleJsonRoute(mux, "/partitions/", app.PartitionsRoute)
}
