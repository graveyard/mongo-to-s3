include golang.mk
include sfncli.mk
.DEFAULT_GOAL := test

SHELL := /bin/bash
PKG := github.com/Clever/mongo-to-s3
PKGS := $(shell go list ./... | grep -v /vendor)
EXECUTABLE = $(shell basename $(PKG))
SFNCLI_VERSION := latest

.PHONY: test $(PKGS) run install_deps build

$(eval $(call golang-version-check,1.9))

# test vars
export SERVICE_GEARMAN_ADMIN_HTTP_PROTO?=x
export SERVICE_GEARMAN_ADMIN_HTTP_PORT?=x
export SERVICE_GEARMAN_ADMIN_HTTP_HOST?=x
export GEARMAN_ADMIN_USER?=x
export GEARMAN_ADMIN_PASS?=x
export GEARMAN_ADMIN_PATH?=x
export IL_CONFIG?=x
export SIS_CONFIG?=x
export APP_SIS_CONFIG?=x
test: $(PKGS)

build: bin/sfncli
	$(call golang-build,$(PKG),$(EXECUTABLE))

run: build
	bin/sfncli --activityname $(_DEPLOY_ENV)--$(_APP_NAME) \
		--region us-west-2 \
		--cloudwatchregion us-west-1 \
		--workername `hostname` \
		--cmd bin/$(EXECUTABLE)

$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)

install_deps: golang-dep-vendor-deps
	$(call golang-dep-vendor)
