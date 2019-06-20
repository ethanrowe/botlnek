package rest

import (
	"net/http"
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

func HandleJsonRoute(mux *http.ServeMux, pattern string, h func(*http.Request) (JsonResponder, error)) {
	mux.Handle(pattern, NewJsonHandler(h))
}

func ApplyRoutes(mux *http.ServeMux) {
	HandleJsonRoute(mux, "/", HelloWorldRoute)
}
