#!/bin/bash

# Settings
pguser='postgres'
pgpass=default.pgpass
nodelist=('localhost' 'localhost' 'localhost' 'localhost' 'localhost' 'localhost' 'localhost' 'localhost')
dblist=('tpch8_1' 'tpch8_2' 'tpch8_3' 'tpch8_4' 'tpch8_5' 'tpch8_6' 'tpch8_7' 'tpch8_8')
datasets='datasets'
errorlog=`basename $0`'.log'
create_schema='create_schema.sql'
create_aggs='create_aggs.sql'
bloom='pg_bloom.sql'
#create_dw='create_dw.sql'

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
	echo "By default, scale = 0.1 and nodes = \"1 2 3 ...\" (all nodes specified in settings), which will upload and partition database of scale 0.1 to all nodes."
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
nodes=(${nodelist[@]})
dbs=(${dblist[@]})
numNodes=(${#nodelist[@]})
if [ "$#" -gt 1 ]
then
	nodes=()
	dbs=()
	numNodes=0
	for i in $2
	do
		nodes[$numNodes]="${nodelist[`expr $i - 1`]}"
		dbs[$numNodes]="${dblist[`expr $i - 1`]}"
		numNodes=`expr $numNodes + 1`
	done
fi

# Empty log file
rm -f $errorlog
touch $errorlog

# Record pgpass
export PGPASSFILE=$pgpass
chmod 600 $pgpass

# Check connectivity
echo "Checking connectivity to nodes..."
for i in $(seq 0 `expr $numNodes - 1`);
do
	(
		command=`psql -h ${nodes[$i]} -U $pguser --set ON_ERROR_STOP=1 2>&1`
		if [ $? -ne 0 ]
		then
			echo "Error connecting to database ${dbs[$i]} at ${nodes[$i]}: $command."
		fi
	) >> $errorlog &
done
wait
checklog

# Create databases if needed
echo "Checking databases..."
for i in $(seq 0 `expr $numNodes - 1`);
do
	(
		command=`psql -h ${nodes[$i]} -U $pguser -c '\l' --set ON_ERROR_STOP=1 | grep " ${dbs[$i]} " 2>&1`
		if [ $? -ne 0 -a $? -ne 1 ]
		then
			echo "Error checking for exitence of database ${dbs[$i]} at ${nodes[$i]}: $command."
		elif [ "$command" == "" ]
		then
			command=`psql -h ${nodes[$i]} -U $pguser -c "CREATE DATABASE ${dbs[$i]}" --set ON_ERROR_STOP=1 2>&1`
			if [ $? -ne 0 ]
			then
				echo "Error creating database ${dbs[$i]} at ${nodes[$i]}: $command."
			fi
		fi
	) >> $errorlog &
done
wait
checklog

# Drop existing tables
echo "Dropping existing tables..."
for i in $(seq 0 `expr $numNodes - 1`);
do
	(
		tables=$(psql -h ${nodes[$i]} -U $pguser -d ${dbs[$i]} -c '\d' --set ON_ERROR_STOP=1 | grep ' table ' | awk '{ print $3 }')
		for table in $tables
		do
			command=`psql -h ${nodes[$i]} -U $pguser -d ${dbs[$i]} -c "DROP TABLE $table" --set ON_ERROR_STOP=1 2>& 1`
			if [ $? -ne 0 ]
			then
				echo "Error dropping table $table in database ${dbs[$i]} at ${nodes[$i]}: $command."
			fi
		done
	) >> $errorlog &
done
wait
checklog

# Create schema
echo "Creating schema..."
if [ ! -f $create_schema ]
then
	echo "Schema file $create_schema does not exist."
	echo "Exiting..."
	exit 1
fi
for i in $(seq 0 `expr $numNodes - 1`);
do
	(
		command=`psql -h ${nodes[$i]} -U $pguser -d ${dbs[$i]} -f "$create_schema" --set ON_ERROR_STOP=1 2>&1`
		if [ $? -ne 0 ]
		then
			echo "Error creating schema in database ${dbs[$i]} at ${nodes[$i]}: $command."
		fi
	) >> $errorlog &
done
wait
checklog

# Create supporting agg functions
echo "Creating supporting aggregate functions..."
if [ ! -f $create_aggs ]
then
	echo "Supporting aggregates file $create_aggs does not exist."
	echo "Exiting..."
	exit 1
fi
for i in $(seq 0 `expr $numNodes - 1`);
do
	(
		command=`psql -h ${nodes[$i]} -U $pguser -d ${dbs[$i]} -f "$create_aggs" --set ON_ERROR_STOP=1 2>&1`
		if [ $? -ne 0 ]
		then
			echo "Error creating supporting aggregates in database ${dbs[$i]} at ${nodes[$i]}: $command."
		fi
	) >> $errorlog &
done
wait
checklog

# Create bloom functions
echo "Creating bloom functions..."
if [ ! -f $bloom ]
then
	echo "Bloom functions file $bloom does not exist."
	echo "Exiting..."
	exit 1
fi
for i in $(seq 0 `expr $numNodes - 1`);
do
	(
		command=`psql -h ${nodes[$i]} -U $pguser -d ${dbs[$i]} -f "$bloom" --set ON_ERROR_STOP=1 2>&1`
		if [ $? -ne 0 ]
		then
			echo "Error creating bloom functions in database ${dbs[$i]} at ${nodes[$i]}: $command."
		fi
	) >> $errorlog &
done
wait
checklog

# Create dw
: <<'COMMENT2'
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
COMMENT2

# Fix pipe character at the end of lines, prepare import commands
echo "Preparing data..."
importfile=`basename $0`.tmp
rm -f $importfile
tbls=`ls $datasets/$scale | grep -e .tbl$`
for tbl in $tbls
do
	cat $datasets/$scale/$tbl | sed 's/|$//' > $datasets/$scale/$tbl.tmp;
	mv $datasets/$scale/$tbl.tmp $datasets/$scale/$tbl;
	echo "\copy ${tbl%'.tbl'} from '$datasets/$scale/$tbl' DELIMITER '|'" >> $importfile
done

# Load data
echo "Loading data..."
curNodeNum=0

for i in $(seq 0 `expr $numNodes - 1`);
do
	curNodeNum=`expr $curNodeNum + 1`
	(
		command=`psql -h ${nodes[$i]} -U $pguser -d "${dbs[$i]}" -f "$importfile" 2>&1`
		# below doesn't work
		#if [ $? -ne 0 ]
		#then
		#	echo "Error importing data to database ${dbs[$i]} at ${nodes[$i]}: $command."
		#fi
	) >> $errorlog &
done
wait
checklog
rm -f $importfile
	
# Partition: horizontal for customers, orders, and lineitems; none for others - all on first node
curNodeNum=0
echo "Partitioning..."
for i in $(seq 0 `expr $numNodes - 1`);
do
	partitionSize=`echo $scale*150000/$numNodes | bc -l` # there are 150,000 tuples in customers table with scale 1
	(
		query="DELETE FROM customer WHERE c_custkey NOT BETWEEN `echo "($i*$partitionSize+1)/1" | bc` AND `echo "(($i+1)*$partitionSize)/1" | bc`;"
		query="$query DELETE FROM orders WHERE o_custkey NOT BETWEEN `echo "($i*$partitionSize+1)/1" | bc` AND `echo "(($i+1)*$partitionSize)/1" | bc`;"
		query="$query DELETE FROM lineitem WHERE l_orderkey NOT IN (SELECT o_orderkey FROM orders);"
		if [ $i -ne 0 ]
		then
			query="$query TRUNCATE region; TRUNCATE nation; TRUNCATE part; TRUNCATE partsupp; TRUNCATE supplier;"
		fi
		command=`psql -h ${nodes[$i]} -U $pguser -d "${dbs[$i]}" -c "$query" --set ON_ERROR_STOP=1 2>&1`
		if [ $? -ne 0 ]
		then
			echo "Error partitioning data in database ${dbs[$i]} at ${nodes[$i]}: $command."
		fi
	) >> $errorlog &
done
wait
checklog

echo "All done."

# Remove log file
rm -f $errorlog
