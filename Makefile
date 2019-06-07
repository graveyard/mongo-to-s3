include golang.mk
include sfncli.mk
.DEFAULT_GOAL := test

SHELL := /bin/bash
PKG := github.com/Clever/mongo-to-s3
PKGS := $(shell go list ./... | grep -v /vendor)
EXECUTABLE = $(shell basename $(PKG))
SFNCLI_VERSION := latest

.PHONY: test $(PKGS) run install_deps build

$(eval $(call golang-version-check,1.12))

# test vars
export SERVICE_GEARMAN_ADMIN_HTTP_PROTO?=x
export SERVICE_GEARMAN_ADMIN_HTTP_PORT?=x
export SERVICE_GEARMAN_ADMIN_HTTP_HOST?=x
export GEARMAN_ADMIN_USER?=x
export GEARMAN_ADMIN_PASS?=x
export GEARMAN_ADMIN_PATH?=x
export IL_URL?=x
export IL_USERNAME?=x
export IL_PASSWORD?=x
export IL_CONFIG?=x
export SIS_URL?=x
export SIS_USERNAME?=x
export SIS_PASSWORD?=x
export SIS_CONFIG?=x
export SIS_READ_URL?=x
export SIS_READ_USERNAME?=x
export SIS_READ_PASSWORD?=x
export SIS_READ_CONFIG?=x
export APP_SIS_URL?=x
export APP_SIS_CONFIG?=x
export APP_SIS_READ_URL?=x
export APP_SIS_READ_CONFIG?=x
export LEGACY_URL?=x
export LEGACY_CONFIG?=x
export LEGACY_READ_URL?=x
export LEGACY_READ_CONFIG?=x
test: $(PKGS)

build: bin/sfncli
	$(call golang-build,$(PKG),$(EXECUTABLE))

run: build
	bin/$(EXECUTABLE) '$(PAYLOAD)'

$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)

install_deps: golang-dep-vendor-deps
	$(call golang-dep-vendor)
