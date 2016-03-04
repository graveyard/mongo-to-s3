include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: all build clean test $(PKGS)
SHELL := /bin/bash
PKG := github.com/Clever/mongo-to-s3
SUBPKGS := $(addprefix $(PKG)/, aws fab config)
PKGS := $(PKG) $(SUBPKGS)
GOLINT := $(GOPATH)/bin/golint

$(eval $(call golang-version-check,1.5))

test: $(PKGS)

all: build test

$(GOLINT):
	go get github.com/golang/lint/golint

build: clean
	GO15VENDOREXPERIMENT=1 go build -o "mongo-to-s3" $(PKG)

clean:
	rm -f mongo-to-s3
	rm -f c.out
	rm -f config/c.out

$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)

vendor: golang-godep-vendor-deps
	$(call golang-godep-vendor,$(PKGS))
