# derived from https://github.com/rootless-containers/bypass4netns/blob/c60261fb565e09a901f477d579eda2923fe65427/Makefile
# which is licensed under Apache Version 2.0
GO ?= go
PACKAGE := github.com/naoki9911/bcachefs_exporter
VERSION=$(shell git describe --match 'v[0-9]*' --dirty='.m' --always --tags)
VERSION_TRIMMED := $(VERSION:v%=%)
GO_BUILD_FLAGS += -trimpath
GO_BUILD_LDFLAGS += -s -w -X $(PACKAGE)/pkg/version.Version=$(VERSION)
GO_BUILD := $(GO) build $(GO_BUILD_FLAGS) -ldflags "$(GO_BUILD_LDFLAGS)"
GO_BUILD_STATIC := CGO_ENABLED=1 $(GO) build $(GO_BUILD_FLAGS) -tags "netgo osusergo" -ldflags "$(GO_BUILD_LDFLAGS) -extldflags -static"

.DEFAULT: all

all: exporter

exporter:
	$(GO_BUILD_STATIC) ./cmd/exporter

install:
	install exporter /usr/local/bin/bcachefs_exporter
	cp bcachefs_exporter.service /etc/systemd/system/bcachefs_exporter.service

uninstall:
	rm -rf /usr/local/bin/bcachefs_exporter
	rm -rf /etc/systemd/system/bcachefs_exporter.service

clean:
	rm -rf exporter

.PHONY: all exporter install uninstall clean
