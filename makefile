SHELL=/bin/bash

GO_BUILD = go build
GO_TEST = go test

SOURCES := $(shell find . -name '*.go' -not -name '*_test.go')
TEST_PACKAGES := $(shell find . -path '*_test.go' -printf '%h ')
TEST_FILES := $(shell find -name '*.go')

CMDS := \
	bin/restserver

$(CMDS): $(SOURCES)
	$(GO_BUILD) -o $@ ./cmd/$(shell basename "$@")


build: $(CMDS)

test: $(SOURCES) $(TEST_FILES)
	$(GO_TEST) $(TEST_PACKAGES)


clean:
	rm bin/*

