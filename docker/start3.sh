#!/bin/sh

idx=$1
user=$2
try=$3
shift
shift
shift
java -DlogFilename=logs/node$idx-t3-$try -cp asdProj.jar Main -conf config.properties dissemination_and_membership=0 payload_size=1024 broadcast_interval=600 "$@" &> /proc/1/fd/1
chown $user logs/node$idx-t3-$try.log
