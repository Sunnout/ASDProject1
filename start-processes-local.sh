#!/bin/bash

processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
	        echo "please indicate a number of processes of at least one"
		        exit 0
fi

i=0
port=5000

(java -jar target/asdProj.jar -conf config.properties address=$(hostname) port=$port | tee results/results-$(hostname)-$[$port+$i].txt)&
echo "launched contact on port $port"
sleep 3

i=1

while [ $i -lt $processes ]
do
	(java -jar target/asdProj.jar -conf config.properties address=$(hostname) port=$[$port+$i] contact=$(hostname):$port | tee results/results-$(hostname)-$[$port+$i].txt)&
	echo "launched process on port $[$port+$i]"
	sleep 1
	i=$[$i+1]	
done
