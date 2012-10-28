package ch.epfl.ad.bloomjoin;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ch.epfl.ad.db.DatabaseManager;

/**
 * Bloom join operator implementation 
 * 
 * @author Stanislav Peshterliev
 */
public class BloomJoin {
	private DatabaseManager dbManager;
	private final ExecutorService pool;
	
	public BloomJoin(DatabaseManager dbManager) {
		this.dbManager = dbManager;
		this.pool = Executors.newCachedThreadPool();
	}

	/**
	 * Perfrom bloom join on two relations
	 * 
	 * @param leftRelation - the left relation of the join, should be entirely located on single node, 
	 * 						 this is the relation that we filter based on bloom filter and ship the results 
	 * 						 to the right relation
	 * @param leftColumn - the join column of the left relation
	 * @param leftNodeId - the node where left relation is located
	 * 
	 * @param rightRelation - right relation of the join, might be located on multiple nodes, 
	 * 						  this is the relation that we compute the bloom filter on and send it 
	 * 						  to left relation node
	 * @param rightColumn - the join column of the right relation
	 * @param rightNodeIds - the node where left relation is located
	 * 
	 * @param joinQuery - this is a standard SQL join query which includes left relation and right relation, 
	 * 					  and the corresponding join columns 
	 * @param resultTableSchema - the name of the resulting table schema
	 * @param destinationNodeId - destination node to save the results
	 */
	public void join(final String leftRelation, final String leftColumn, final String leftNodeId,
					 final String rightRelation, final String rightColumn, final List<String> rightNodeIds, 
					 final String joinQuery,
					 final String resultTableSchema,
					 final String destinationNodeId) throws SQLException, InterruptedException {
		
		final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections.synchronizedList(new ArrayList<SQLException>());
        
		for (final String rightNodeId : rightNodeIds) {
			tasks.add(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
                    try {
                    	final String bloomTableName = "bloom_" + rightRelation + "_" + rightNodeId;
                    	
            			// ensure that temporary tables do not exist
                    	BloomJoin.this.dbManager.execute("DROP TABLE IF EXISTS " + bloomTableName, leftNodeId);
                    	BloomJoin.this.dbManager.execute("DROP TABLE IF EXISTS " + "temp_" + leftRelation, rightNodeId);
                    	
                    	ResultSet rs = BloomJoin.this.dbManager.fetch("SELECT COUNT(*) FROM " + rightRelation, rightNodeId);
                    	rs.next();
                    	int rightRelationCount = rs.getInt(1);
                    	
                    	BloomJoin.this.dbManager.execute(
                    			String.format("CALL createemptybloomfilter('%s')", bloomTableName),
                    			leftNodeId
                    			);

            			// create bloom filter on the right relation and copy it to left node
                    	BloomJoin.this.dbManager.execute(
            					String.format("CALL computebloomfilter(%s, '%s', 'SELECT %s FROM %s')",
            							rightRelationCount, rightColumn, rightColumn, rightRelation
            					),
            					rightNodeId,
            					bloomTableName,
            					leftNodeId
            			);
                    	
            			// apply the bloom join on the left node and ship the result to the right node
                    	BloomJoin.this.dbManager.execute(
            					String.format("call filterbybloom(%s, '%s', 'SELECT * FROM %s WHERE ?', '%s')",
            							rightRelationCount, leftColumn, leftRelation, bloomTableName
            					), 
            					leftNodeId,
            					"temp_" + leftRelation,
            					rightNodeId
            			);
                    	
                    	// check if after the bloom join there are tuples to join with                    	
                    	ResultSet showTableRs = BloomJoin.this.dbManager.fetch(
                    			String.format("SHOW TABLES LIKE '%s'", "temp_" + leftRelation), 
                    			rightNodeId
                    		);
                    	
                    	if (showTableRs.next()) {
	            			// do a join between left relation and right relation with left relation 
	            			// being the reduced temporary relation shipped to the right node
	                    	BloomJoin.this.dbManager.execute(
	            				joinQuery.replaceAll("\\b" + leftRelation + "\\b", "temp_" + leftRelation),				
	            				rightNodeId,
	            				resultTableSchema,
	            				destinationNodeId
	            			);
                    	}
            			
            			// clean up temporary tables
                    	BloomJoin.this.dbManager.execute("DROP TABLE IF EXISTS " + bloomTableName, leftNodeId);
                    	BloomJoin.this.dbManager.execute("DROP TABLE IF EXISTS " + "temp_" + leftRelation, rightNodeId);

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
	
	public void shutDown() {
        this.pool.shutdown();
	}
}
