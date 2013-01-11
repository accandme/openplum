package ch.epfl.ad.bloomjoin;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ch.epfl.ad.db.AbstractDatabaseManager;
import ch.epfl.ad.db.DatabaseManager;

/**
 * SuperDuper operator implementation 
 * 
 * @author Amer C <amer.chamseddine@epfl.ch>
 */
public class SuperDuper {
	private DatabaseManager dbManager;
	private final ExecutorService pool;
	private final int superDuperId;
	
	public SuperDuper(DatabaseManager dbManager) {
		this.dbManager = dbManager;
		this.pool = Executors.newCachedThreadPool();
		superDuperId = new Random().nextInt(1000000);
	}
	
	/**
	 * Performs SuperDuper on two relations
	 */
	public void runSuperDuper(final List<String> fromNodeIds, final List<String> toNodeIds, 
			final String fromRelation, final String toRelation,
			final String fromColumn, final String toColumn,
			final String outRelation) throws SQLException, InterruptedException {
		
		final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections.synchronizedList(new ArrayList<SQLException>());
        
		for (final String nodeId : toNodeIds) {
			tasks.add(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
                    try {
                    	final String bloomFilterTableName = "bloomfilter_" + superDuperId + "_" + nodeId;
                    	
                    	// create holder
                    	SuperDuper.this.dbManager.execute("DROP TABLE IF EXISTS " + bloomFilterTableName, fromNodeIds);
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
                    	SuperDuper.this.dbManager.execute("DROP TABLE IF EXISTS " + bloomFilterTableName, fromNodeIds);
                    	

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
