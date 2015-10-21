REPO            ?= machi
PKG_REVISION    ?= $(shell git describe --tags)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR := $(shell which rebar)
ifeq ($(REBAR),)
REBAR = $(BASE_DIR)/rebar
endif
OVERLAY_VARS    ?=

.PHONY: rel deps package pkgclean edoc

all: deps compile

compile:
	$(REBAR) compile

## Make reltool happy by creating a fake entry in the deps dir for
## machi, because reltool really wants to have a path with
## "machi/ebin" at the end, but we also don't want infinite recursion
## if we just symlink "deps/machi" -> ".."

generate:
	rm -rf deps/machi
	mkdir deps/machi
	ln -s ../../ebin deps/machi
	ln -s ../../src deps/machi
	$(REBAR) generate $(OVERLAY_VARS) 2>&1 | grep -v 'command does not apply to directory'

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) -r clean

test: deps compile eunit

eunit:
	$(REBAR) -v skip_deps=true eunit

edoc: edoc-clean
	$(REBAR) skip_deps=true doc

edoc-clean:
	rm -f edoc/*.png edoc/*.html edoc/*.css edoc/edoc-info

pulse: compile
	@echo Sorry, PULSE test needs maintenance. -SLF
	#env USE_PULSE=1 $(REBAR) skip_deps=true clean compile
	#env USE_PULSE=1 $(REBAR) skip_deps=true -D PULSE eunit -v

APPS = kernel stdlib sasl erts ssl compiler eunit crypto
PLT = $(HOME)/.machi_dialyzer_plt

build_plt: deps compile
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS) deps/*/ebin

DIALYZER_DEP_APPS = ebin/machi_pb.beam \
		    deps/cluster_info/ebin \
		    deps/protobuffs/ebin \
		    deps/riak_dt/ebin
DIALYZER_FLAGS = -Wno_return -Wrace_conditions -Wunderspecs

dialyzer: deps compile
	dialyzer $(DIALYZER_FLAGS) --plt $(PLT) ebin $(DIALYZER_DEP_APPS) | \
	    tee ./.dialyzer-last-run.txt | \
            egrep -v -f ./filter-dialyzer-dep-warnings

dialyzer-test: deps compile
	echo Force rebar to recompile .eunit dir w/o running tests > /dev/null
	rebar skip_deps=true eunit suite=lamport_clock
	dialyzer $(DIALYZER_FLAGS) --plt $(PLT) .eunit $(DIALYZER_DEP_APPS) | \
            egrep -v -f ./filter-dialyzer-dep-warnings

clean_plt:
	rm $(PLT)

##
## Release targets
##
rel: deps compile generate

relclean:
	rm -rf rel/$(REPO)

stage : rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/$(REPO)/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) rel/$(REPO)/lib;)
