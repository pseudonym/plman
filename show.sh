#!/bin/sh
# show all hosts in DHT

if test $# != 1; then
	echo "Usage: $0 host:port"
fi

host="$(echo $1 | cut -f1 -d:)"
port="$(echo $1 | cut -f2 -d:)"

echo CSHOW | nc $host $port |
while read cmd hash net; do
	if test $cmd = CPEER; then
		echo $hash $net
	fi
done
