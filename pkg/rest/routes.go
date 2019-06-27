package rest

import (
	"errors"
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
	DomainWriter model.DomainWriter
	DomainReader model.DomainReader
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
	if r.Method == http.MethodPost {
		// For now just accept.
		return NewJsonResponse(202, "partition accepted"), nil
	}
	return NewJsonErrorResponse(http.StatusMethodNotAllowed, errors.New("Only POST supported")), nil
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
