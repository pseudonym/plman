#!/bin/sh
# gets a value and prints it

if test $# != 2; then
	echo "Usage: $0 address:port hash"
fi

host="$(echo $1 | cut -f1 -d:)"
port="$(echo $1 | cut -f2 -d:)"
hash=$2

echo CGET $2 |
nc $host $port |
while read type rest
do
	if test "$type" = CERROR; then
		echo error: $rest >&2
	elif test "$type" = CDATA; then
		echo $rest | base64 -d
	fi
	exit
done
