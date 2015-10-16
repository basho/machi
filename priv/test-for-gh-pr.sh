#!/bin/sh

set -x

if [ "${TRAVIS_PULL_REQUEST}" = "false" ] then
	echo '$TRAVIS_PULL_REQUEST is false, skipping tests'
	exit 0
else
	make test
fi
