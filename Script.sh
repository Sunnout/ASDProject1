#!/bin/bash



for TH in {5001..5009}
do	
	myScript='java -jar target/asdProj.jar -conf config.properties contact=127.0.0.1:5000 port="'"$TH"'"; exec bash'
	script='echo "arg 1 is:"'" $TH"'"";exec bash  '
	gnome-terminal -- /bin/sh -c "$myScript"

done

