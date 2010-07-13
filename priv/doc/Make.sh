#!/bin/sh

for i in *.msc
do
 mscgen -T png -i $i -o ./`basename $i .msc`.png
 mscgen -T svg -i $i -o ./`basename $i .msc`.svg
done
