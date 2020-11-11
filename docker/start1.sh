#!/bin/sh

idx=$1
user=$2
shift
shift
java -DlogFilename=logs/node$idx-t1 -cp asdProj.jar Main -conf config.properties dissemination_and_membership=3 "$@" &> /proc/1/fd/1
chown $user logs/node$idx-t1.log
