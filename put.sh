#!/bin/sh
# puts a value into the DHT

if test $# != 1; then
	echo "Usage: cat file | $0 host:port"
fi

host="$(echo $1 | cut -f1 -d:)"
port="$(echo $1 | cut -f2 -d:)"
# base64 encode the data
data="$(base64 -w 0)"

echo CPUT "$data" |
nc $host $port |
while read cmd rest # for some reason, I have to use a while here? stupid shell, making no sense
do
	if test "$cmd" = COK; then
		echo $rest inserted
	elif test "$cmd" = CERROR; then
		echo error: $rest >&2
	fi
	exit
done
