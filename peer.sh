#!/bin/sh
# driver program
if test $# != 2; then
	echo "Usage: $0 listen_port bootstraphost:bootstrapport"
	exit 1
fi

port="$1"
bootstrap="$2"

exec python peer.py "$(curl -s http://128.253.240.101/ip.php):$port" "$bootstrap"
