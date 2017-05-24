include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: all build clean test $(PKGS)
SHELL := /bin/bash
PKG := github.com/Clever/mongo-to-s3
SUBPKGS := $(addprefix $(PKG)/, aws fab config)
PKGS := $(PKG) $(SUBPKGS)
GOLINT := $(GOPATH)/bin/golint
NUMFILES?=1

$(eval $(call golang-version-check,1.8))

export SERVICE_GEARMAN_ADMIN_HTTP_PROTO?=x
export SERVICE_GEARMAN_ADMIN_HTTP_PORT?=x
export SERVICE_GEARMAN_ADMIN_HTTP_HOST?=x
export GEARMAN_ADMIN_USER?=x
export GEARMAN_ADMIN_PASS?=x
export GEARMAN_ADMIN_PATH?=x
test: $(PKGS)

all: build test

$(GOLINT):
	go get github.com/golang/lint/golint

build: clean
	GO15VENDOREXPERIMENT=1 go build -o "mongo-to-s3" $(PKG)

run: build
	./mongo-to-s3 -database $(DATABASE) -bucket clever-analytics-dev -config $(CONFIG) -collections $(COLLECTIONS) -numfiles $(NUMFILES)

clean:
	rm -f mongo-to-s3
	rm -f c.out
	rm -f config/c.out

$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)

vendor: golang-godep-vendor-deps
	$(call golang-godep-vendor,$(PKGS))
