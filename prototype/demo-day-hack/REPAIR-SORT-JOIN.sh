#!/bin/sh

trap "rm -f $1.tmp $2.tmp" 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15

if [ $# -ne 6 ]; then
	echo usage: $0 1. 2. 3. 4. 5. 6.
	exit 1
fi

sort -u $1 > $1.tmp
sort -u $2 > $2.tmp
cmp -s $1.tmp $2.tmp
if [ $? -eq 0 ]; then
	## Output is identical.
	touch $6
	exit 0
fi

# print only lines joinable on field 1 (offset)
join      -1 1 -2 1 $1.tmp $2.tmp > $3

# print only lines field 1 (offset) present only in file 1
join -v 1 -1 1 -2 1 $1.tmp $2.tmp > $4

# print only lines field 1 (offset) present only in file 2
join -v 2 -1 1 -2 1 $1.tmp $2.tmp > $5
