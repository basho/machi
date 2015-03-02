#!/bin/sh

ENCDEC=$1		# Should be 'encoder' or 'decoder'
shift
WORK_DIR=$1
shift

case $0 in
    */*)
        MY_DIR=`dirname $0`
	;;
    *)
	TMP=`which $0`
	MY_DIR=`dirname $TMP`
	;;
esac

cd $MY_DIR
env CODING_DIR=$WORK_DIR ./$ENCDEC.bin "$@" > /dev/null 2>&1
echo $?
