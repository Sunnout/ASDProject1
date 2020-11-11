#!/bin/sh

idx=$1
user=$2
shift
shift
java -DlogFilename=logs/node$idx-t2-01 -cp asdProj.jar Main -conf config.properties dissemination_and_membership=1 "$@" &> /proc/1/fd/1
chown $user logs/node$idx-t2-01.log
