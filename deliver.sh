#!/bin/sh
# script to copy payload to remote server and start the bootstrap script

# host to connect to
host="$1"
user=cornell_cs6460
# network address of central control server
control="$2"

files="*.py *.sh"
ssh_opts="-q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# ssh is so awesome
tar c $files | ssh $ssh_opts -l $user $host "cd \`mktemp -d\` && tar x && sh -c 'python peer.py $host $control >/dev/null &'"
