SHELL=/bin/bash

GO_BUILD = go build

SOURCES := $(shell find . -name '*.go' -not -name '*_test.go')
CMDS := \
	bin/restserver

$(CMDS): $(SOURCES)
	$(GO_BUILD) -o $@ ./cmd/$(shell basename "$@")


build: $(CMDS)

clean:
	rm bin/*

