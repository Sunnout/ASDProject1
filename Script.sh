#!/bin/bash



for TH in {0..3}
do
	script='echo "arg 1 is:"'" $TH"'"";exec bash  '
	gnome-terminal -- /bin/sh -c "$script"
done

