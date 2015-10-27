#!/bin/sh

if [ "${TRAVIS_PULL_REQUEST}" = "false" ]; then
	echo '$TRAVIS_PULL_REQUEST is false, skipping tests'
	exit 0
else
	echo '$TRAVIS_PULL_REQUEST is not false ($TRAVIS_PULL_REQUEST), running tests'
	make test
	make dialyzer
fi
