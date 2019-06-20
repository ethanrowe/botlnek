package rest

import (
	"encoding/json"
	"net/http"
	"strconv"
)

type JsonResponder interface {
	json.Marshaler
	StatusCode() int
}

type jsonResponse struct {
	payload    interface{}
	statusCode int
}

func (j *jsonResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&j.payload)
}

func (j *jsonResponse) StatusCode() int {
	return j.statusCode
}

func NewJsonResponse(code int, payload interface{}) JsonResponder {
	return &jsonResponse{payload: payload, statusCode: code}
}

type jsonError struct {
	err        error
	statusCode int
}

type errMsg struct {
	Error string
}

func (je jsonError) MarshalJSON() ([]byte, error) {
	return json.Marshal(errMsg{Error: je.Error()})
}

func (je jsonError) Error() string {
	return je.err.Error()
}

func (je jsonError) StatusCode() int {
	return je.statusCode
}

func NewJsonErrorResponse(code int, err error) JsonResponder {
	return &jsonError{err: err, statusCode: code}
}

type JsonHandler struct {
	jsonHandler func(*http.Request) (JsonResponder, error)
}

func (h JsonHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	result, err := h.jsonHandler(r)
	if err != nil {
		result = NewJsonErrorResponse(500, err)
	}
	body, err := result.MarshalJSON()

	if err != nil {
		// respond with a plaintext error since we failed to marhal
		// to JSON.
		http.Error(w, err.Error(), 500)
	}

	// Set the headers first lest they become trailers
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(result.StatusCode())
	w.Write(body)
}

func NewJsonHandler(handler func(*http.Request) (JsonResponder, error)) http.Handler {
	return JsonHandler{jsonHandler: handler}
}
