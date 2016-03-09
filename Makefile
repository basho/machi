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
EUNIT_OPTS       = -v

.PHONY: rel stagedevrel deps package pkgclean edoc

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

edoc: edoc-clean
	$(REBAR) skip_deps=true doc

edoc-clean:
	rm -f edoc/*.png edoc/*.html edoc/*.css edoc/edoc-info

pulse: compile
	@echo Sorry, PULSE test needs maintenance. -SLF
	#env USE_PULSE=1 $(REBAR) skip_deps=true clean compile
	#env USE_PULSE=1 $(REBAR) skip_deps=true -D PULSE eunit -v

##
## Release targets
##
rel: deps compile generate

relclean:
	rm -rf rel/$(REPO)

stage : rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/$(REPO)/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) rel/$(REPO)/lib;)

##
## Developer targets
##
##  devN - Make a dev build for node N
##  stagedevN - Make a stage dev build for node N (symlink libraries)
##  devrel - Make a dev build for 1..$DEVNODES
##  stagedevrel Make a stagedev build for 1..$DEVNODES
##
##  Example, make a 68 node devrel cluster
##    make stagedevrel DEVNODES=68

.PHONY : stagedevrel devrel
DEVNODES ?= 3

# 'seq' is not available on all *BSD, so using an alternate in awk
SEQ = $(shell awk 'BEGIN { for (i = 1; i < '$(DEVNODES)'; i++) printf("%i ", i); print i ;exit(0);}')

$(eval stagedevrel : $(foreach n,$(SEQ),stagedev$(n)))
$(eval devrel : $(foreach n,$(SEQ),dev$(n)))

dev% : all
	mkdir -p dev
	rel/gen_dev $@ rel/vars/dev_vars.config.src rel/vars/$@_vars.config
	(cd rel && ../rebar generate target_dir=../dev/$@ overlay_vars=vars/$@_vars.config)

stagedev% : dev%
	  $(foreach dep,$(wildcard deps/*), rm -rf dev/$^/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) dev/$^/lib;)

devclean: clean
	rm -rf dev

DIALYZER_APPS = kernel stdlib sasl erts ssl compiler eunit crypto public_key syntax_tools
PLT = $(HOME)/.machi_dialyzer_plt

include tools.mk
