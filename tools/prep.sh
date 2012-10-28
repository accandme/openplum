#!/bin/bash

# Settings
pass='thrufbeyto'
db='dbcourse1'
nodelist=('icdatasrv1-9' 'icdatasrv1-20' 'icdatasrv2-9' 'icdatasrv2-20' 'icdatasrv3-9' 'icdatasrv3-20' 'icdatasrv4-9' 'icdatasrv4-20')
datasets='datasets'
errorlog=`basename $0`'.log'
create_schema='create_schema.sql'
create_dw='create_dw.sql'

checklog() { # function to check if logfile is empty
	if [ -s $errorlog ]
	then
		echo "Errors occured:"
		cat $errorlog
		rm -f $errorlog
		echo "Exiting..."
		exit 1
	fi
}

# Usage
if [ "$#" -gt 2 -o "$1" = '-?' -o "$1" = '--help' ]
then
	echo "Usage: $0 [scale] [nodes]"
	echo "Loads data from $datasets/<scale>/ to <nodes> and partitions it among them, specifying the first node to be the NS node."
	echo "By default, scale = 0.1 and nodes = \"1 2 3 4 5 6 7 8\", which will upload and partition database of scale 0.1 to all 8 nodes."
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

# Nodes
nodes="${nodelist[0]} ${nodelist[1]} ${nodelist[2]} ${nodelist[3]} ${nodelist[4]} ${nodelist[5]} ${nodelist[6]} ${nodelist[7]}"
numNodes=8
if [ "$#" -gt 1 ]
then
	nodes=""
	numNodes=0
	for i in $2
	do
		numNodes=`expr $numNodes + 1`
		nodes="$nodes ${nodelist[`expr $i - 1`]}"
	done
	nodes=`echo $nodes | sed "s/^[ \t]*//"` # remove leading space
fi

# Empty log file
rm -f $errorlog
touch $errorlog

# Check connectivity
echo "Checking connectivity to nodes..."
for node in $nodes
do
	(
		command=`mysql --host=$node --password=$pass $db -e 'SHOW TABLES' 2>&1`
		if [ $? != 0 ]
		then
			echo "Error connecting to $node: $command."
		fi
	) >> $errorlog &
done
wait
checklog

# Drop existing tables
echo "Dropping existing tables..."
for node in $nodes
do
	(
		tables=$(mysql --host=$node --password=$pass $db -e 'SHOW FULL TABLES WHERE Table_Type = "BASE TABLE"' | awk '{ print $1 }' | grep -v '^Tables')
		for table in $tables
		do
			command=`mysql --host=$node --password=$pass $db -e "DROP TABLE $table" 2>& 1`
			if [ $? != 0 ]
			then
				echo "Error dropping table $table on $node: $command."
			fi
		done
	) >> $errorlog &
done
wait
checklog

# Create schema
echo "Creating schema..."
for node in $nodes
do
	(
		command=`mysql --host=$node --password=$pass $db < $create_schema 2>&1`
		if [ $? != 0 ]
		then
			echo "Error creating schema on $node: $command."
		fi
	) >> $errorlog &
done
checklog

# Create dw
echo "Creating data warehouse schema..."
for node in $nodes
do
	(
		command=`mysql --host=$node --password=$pass $db < $create_dw 2>&1`
		if [ $? != 0 ]
		then
			echo "Error creating data warehouse schema on $node: $command."
		fi
	) >> $errorlog &
done
wait
checklog

# Load data
curNodeNum=0
echo "Loading data..."
for node in $nodes
do
	curNodeNum=`expr $curNodeNum + 1`
	(
		tbls="$datasets/$scale/*.tbl"
		command=`mysqlimport --host=$node --password=$pass --local --fields-terminated-by='|' $db $tbls 2>&1`
		if [ $? != 0 ]
		then
			echo "Error loading data on $node: $command."
		fi
	) >> $errorlog &
done
wait
checklog
	
# Partition
curNodeNum=0
echo "Partitioning..."
for node in $nodes
do
	curNodeNum=`expr $curNodeNum + 1`
	partitionSize=`echo $scale*150000/$numNodes | bc -l` # there are 150,000 tuples in customers table with scale 1
	(
		query="DELETE FROM customer WHERE c_custkey NOT BETWEEN `echo "(($curNodeNum-1)*$partitionSize+1)/1" | bc` AND `echo "($curNodeNum*$partitionSize)/1" | bc`;"
		query="$query DELETE FROM orders WHERE o_custkey NOT BETWEEN `echo "(($curNodeNum-1)*$partitionSize+1)/1" | bc` AND `echo "($curNodeNum*$partitionSize)/1" | bc`;"
		query="$query DELETE FROM lineitem WHERE l_orderkey NOT IN (SELECT o_orderkey FROM orders);"
		command=`mysql --host=$node --password=$pass $db -e "$query" 2>& 1`
		if [ $? != 0 ]
		then
			echo "Error partitioning on $node: $command."
		fi
	) >> $errorlog &
done
wait
checklog

# Remove log file
rm -f $errorlog
