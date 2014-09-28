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
	env USE_PULSE=1 $(REBAR_BIN) skip_deps=true clean compile
	env USE_PULSE=1 $(REBAR_BIN) skip_deps=true -D PULSE eunit

CONC_ARGS = --pz ./.eunit --treat_as_normal shutdown --after_timeout 1000

concuerror: deps compile
	$(REBAR_BIN) -v skip_deps=true eunit suites=do_not_exist
	concuerror -m machi_flu0_test -t concuerror1_test $(CONC_ARGS) && \
	concuerror -m machi_flu0_test -t concuerror2_test $(CONC_ARGS) && \
	concuerror -m machi_flu0_test -t concuerror3_test $(CONC_ARGS) && \
	concuerror -m machi_flu0_test -t concuerror4_test $(CONC_ARGS)
