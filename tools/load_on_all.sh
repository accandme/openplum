#!/bin/bash

pass='thrufbeyto'
db='dbcourse1'
nodelist=('icdatasrv1-9' 'icdatasrv1-20' 'icdatasrv2-9' 'icdatasrv2-20' 'icdatasrv3-9' 'icdatasrv3-20' 'icdatasrv4-9' 'icdatasrv4-20')
datasets='datasets'
filename="$1"

nodes="${nodelist[0]} ${nodelist[1]} ${nodelist[2]} ${nodelist[3]} ${nodelist[4]} ${nodelist[5]} ${nodelist[6]} ${nodelist[7]}"
numNodes=8

# Check connectivity
for node in $nodes
do
	command=`mysql --host=$node --password=$pass $db -e 'SHOW TABLES' 2>&1`
	if [ $? != 0 ]
	then
		echo "Error connecting to $node: $command."
		echo "Exiting..."
		exit 1
	fi
done

# Go
curNodeNum=0
for node in $nodes
do
	# Load
	echo "Loading $filename on $node..."
	command=`mysql --host=$node --password=$pass $db < $filename 2>&1`
	if [ $? != 0 ]
	then
		echo "Error: $command."
		echo "Exiting..."
		exit 1
	fi
done
