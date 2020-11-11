#!/bin/sh

idx=$1
user=$2
try=$3
shift
shift
shift
java -DlogFilename=logs/node$idx-t16-$try -cp asdProj.jar Main -conf config.properties dissemination_and_membership=3 payload_size=1000000 broadcast_interval=200 "$@" &> /proc/1/fd/1
chown $user logs/node$idx-t16-$try.log
