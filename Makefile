BUILD_DIR         := bin
BINARIES          := $(shell find . -name 'main.go' | grep -v -e vendor |awk -F/ '{print "bin/" $$2}')
DEPENDENCIES      := $(shell find ./vendor -type f)
VERSION           := $(shell git describe --always --tags --dirty)
BUILD_TIME        := $(shell date +%FT%T%z)

$(BUILD_DIR)/%: common/*.go %/*.go $(DEPENDENCIES)
	go build -ldflags="-s -w -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)" -o $@ ./$(notdir $@)

.PHONY: build
build: $(BINARIES)
