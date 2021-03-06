package ch.epfl.data.distribdb.execution;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ch.epfl.data.distribdb.lowlevel.AbstractDatabaseManager;
import ch.epfl.data.distribdb.lowlevel.DatabaseManager;

/**
 * SuperDuper operator implementation 
 * More details about how it works
 * in the description of 
 * the runSuperDuper function
 * 
 * @author Amer C <amer.chamseddine@epfl.ch>
 */
public class SuperDuper {
	/**
	 * Handle to DB manager used to send queries to various nodes
	 */
	private DatabaseManager dbManager;
	/**
	 * Pool of threads used to parallelize the job
	 */
	private final ExecutorService pool;
	
	/**
	 * Constructor - Initializes object with DB manager
	 * 
	 * @param DatabaseManager
	 */
	public SuperDuper(DatabaseManager dbManager) {
		this.dbManager = dbManager;
		this.pool = Executors.newCachedThreadPool();
	}

	/**
	 * Performs SuperDuper on two relations
	 * Sends tuples from the table fromRelation 
	 * which is distributed on fromNodeIds
	 * to the nodes in toNodeIds
	 * such that a tuple is sent to node x 
	 * if it joins with a tuple of table toRelation on node x 
	 * This is achieved by computing BloomFilters on each 
	 * toRelation relation on nodes in toNodesIds
	 * and sending it to all nodes in fromNodeIds
	 * Then filtering the fromRelation relations with every 
	 * received BloomFilter and sending the each result
	 * to the corresponding node
	 * The join condition is an equi-join 
	 * on the fromColumn of table fromRelation
	 * and the toColumn of table toRelation
	 * The shipped tuples are stored in a temporary 
	 * table called outRelation, on all nodes in toNodeIds   
	 * The bloomFilters argument specifies the names of 
	 * the N temporary tables to be used as holders 
	 * of the N bloom filters on each node   
	 * 
	 * @param fromNodeIds
	 * @param toNodeIds
	 * @param fromRelation
	 * @param toRelation
	 * @param fromColumn
	 * @param toColumn
	 * @param Map<String, String> bloomFilters
	 * @param outRelation
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	public void runSuperDuper(final List<String> fromNodeIds, final List<String> toNodeIds, 
			final String fromRelation, final String toRelation,
			final String fromColumn, final String toColumn,
			final Map<String, String> bloomFilters, final String outRelation) 
					throws SQLException, InterruptedException {
		
		final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections.synchronizedList(new ArrayList<SQLException>());
        
		for (final String nodeId : toNodeIds) {
			tasks.add(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
                    try {
                    	String bloomFilterTableName = bloomFilters.get(nodeId);
                    	
                    	// create holder
                    	// no need to drop-if-exists before, table manager ensures that the name is unique 
                    	SuperDuper.this.dbManager.execute(String.format("SELECT createemptybloomfilter('%s')", bloomFilterTableName), fromNodeIds);
                    	
                    	String sampleFromNodeId = fromNodeIds.get(new Random().nextInt(fromNodeIds.size()));
                    	ResultSet rs = SuperDuper.this.dbManager.fetch("SELECT COUNT(DISTINCT " + fromColumn + ") FROM " + fromRelation, sampleFromNodeId);
                    	rs.next();
                    	int fromRelationCount = rs.getInt(1) * fromNodeIds.size();
                    	
                    	ResultSet rs1 = SuperDuper.this.dbManager.fetch("SELECT * FROM " + fromRelation + " WHERE 1=2", sampleFromNodeId);
                    	String fromSchema = AbstractDatabaseManager.tableSchemaFromMetaData(rs1.getMetaData());
                    	
            			// create bloom filter on the toRelation and replicate it on all nodes
                    	SuperDuper.this.dbManager.execute(
            					String.format("SELECT * FROM computebloomfilter(%s, '%s', 'SELECT CAST(%s AS TEXT) FROM %s')",
            							fromRelationCount, toColumn, toColumn, toRelation
            					),
            					nodeId,
            					bloomFilterTableName,
            					fromNodeIds
            			);
                    	
            			// apply the bloom join on the left node and ship the result to the right node
                    	SuperDuper.this.dbManager.execute(
            					String.format("SELECT * FROM filterbybloom(%s, '%s', 'SELECT * FROM %s WHERE ?', '%s') AS tbl(%s)",
            							fromRelationCount, fromColumn, fromRelation, bloomFilterTableName, fromSchema
            					), 
            					fromNodeIds,
            					outRelation,
            					nodeId
            			);
                    	
                    	// cleanup holder
                    	// no need to clean anymore, table manager takes care of it

                    } catch (SQLException e) {
                        exceptions.add(e);
                    }
                    
                    return null;
				}
			});
		}
		
        this.pool.invokeAll(tasks);
        
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
	}
	
	/**
	 * After finishing a SuperDuper operation this has to be called
	 * to shut down the pool of threads
	 * Failing to call this function may result in the main program 
	 * hanging (not exiting)
	 */
	public void shutDown() {
        this.pool.shutdown();
	}
	
}
