package rest

import (
	"errors"
	"github.com/ethanrowe/botlnek/pkg/util"
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

func DomainsCollectionRoute(r *http.Request) (JsonResponder, error) {
	if r.Method == http.MethodGet {
		// For now we'll always respond with an empty list.
		return NewJsonResponse(200, struct{ Domains []string }{make([]string, 0)}), nil
	}
	if r.Method == http.MethodPost {
		// For now stub with a bogus created.
		return NewJsonResponse(202, "ok"), nil
	}
	return NewJsonErrorResponse(http.StatusMethodNotAllowed, errors.New("Only GET/POST are supported")), nil
}

func DomainRoute(r *http.Request) (JsonResponder, error) {
	if r.Method == http.MethodGet {
		// For now respond with a stub domain
		domain := struct {
			DomainKey string
			Attrs     util.StringKVPairs
		}{
			strings.TrimPrefix(r.URL.Path, "/domains/"),
			util.NewStringKVPairs(
				map[string]string{
					"foo": "Foo?",
					"bar": "Bar!",
					"baz": "baz...",
				},
			),
		}

		return NewJsonResponse(200, domain), nil
	}
	return NewJsonErrorResponse(http.StatusMethodNotAllowed, errors.New("Only GET is supported")), nil
}

func PartitionsRoute(r *http.Request) (JsonResponder, error) {
	if r.Method == http.MethodPost {
		// For now just accept.
		return NewJsonResponse(202, "partition accepted"), nil
	}
	return NewJsonErrorResponse(http.StatusMethodNotAllowed, errors.New("Only POST supported")), nil
}

func HandleJsonRoute(mux *http.ServeMux, pattern string, h func(*http.Request) (JsonResponder, error)) {
	mux.Handle(pattern, NewJsonHandler(h))
}

func ApplyRoutes(mux *http.ServeMux) {
	HandleJsonRoute(mux, "/", HelloWorldRoute)
	// Get a list of domains;
	// Post a new domain
	HandleJsonRoute(mux, "/domains", DomainsCollectionRoute)
	// Get a specific domain
	HandleJsonRoute(mux, "/domains/", DomainRoute)
	// Post a new partition
	// Get an existing partition
	HandleJsonRoute(mux, "/partitions/", PartitionsRoute)
}
