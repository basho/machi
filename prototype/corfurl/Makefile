REBAR_BIN := $(shell which rebar)
ifeq ($(REBAR_BIN),)
REBAR_BIN = ./rebar
endif

.PHONY: rel deps package pkgclean

all: deps compile

compile:
	$(REBAR_BIN) compile

deps:
	$(REBAR_BIN) get-deps

clean:
	$(REBAR_BIN) clean

test: deps compile eunit

eunit:
	$(REBAR_BIN) -v skip_deps=true eunit

pulse: compile
	env BITCASK_PULSE=1 $(REBAR_BIN) skip_deps=true clean compile
	env BITCASK_PULSE=1 $(REBAR_BIN) skip_deps=true -D PULSE eunit
