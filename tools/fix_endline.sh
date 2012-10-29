#!/bin/bash

datasets='datasets'

# Usage
if [ "$#" -gt 1 -o "$1" = '-?' -o "$1" = '--help' ]
then
	echo "Usage: $0 [scale]"
	echo "Removes ending pipe character ('|') from DBGEN tables in $datasets/<scale>/."
	echo "By default, scale = 0.1."
	exit 1
fi

# Scale
scale=0.1
if [ $# -gt 0 ]
then
	scale=$1
fi
if [ ! -d $datasets/$scale ]
then
	echo "Dataset $scale does not exist."
	echo "Exiting..."
	exit 1
fi

for i in customer.tbl nation.tbl partsupp.tbl region.tbl lineitem.tbl orders.tbl part.tbl supplier.tbl; do 
	echo Fixing $i;
	cat $datasets/$scale/$i | sed 's/|$//' > $datasets/$scale/$i.tmp;
	mv $datasets/$scale/$i.tmp $datasets/$scale/$i;
done