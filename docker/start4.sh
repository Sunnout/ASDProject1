#!/bin/sh

idx=$1
user=$2
try=$3
shift
shift
shift
java -DlogFilename=logs/node$idx-t4-$try -cp asdProj.jar Main -conf config.properties dissemination_and_membership=0 payload_size=1000000 broadcast_interval=200 "$@" &> /proc/1/fd/1
chown $user logs/node$idx-t4-$try.log
