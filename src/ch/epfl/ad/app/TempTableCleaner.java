package ch.epfl.ad.app;

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
 * Cleans Temporary Tables from all nodes 
 * 
 * @author Amer C <amer.chamseddine@epfl.ch>
 */
public class TempTableCleaner {
	/**
	 * Handle to DB manager used to send query to the various nodes
	 */
	private DatabaseManager dbManager;
	/**
	 * Pool of threads used to parallelize sending queries to nodes
	 */
	private final ExecutorService pool;
	
	/**
	 * Constructor - initializes the object with a handle 
	 * to DB Manager
	 *  
	 * @param DatabaseManager
	 */
	public TempTableCleaner(DatabaseManager dbManager) {
		this.dbManager = dbManager;
		this.pool = Executors.newCachedThreadPool();
	}

	/**
	 * Cleans Temporary Tables on the nodes mentioned 
	 * in the given list of nodes
	 * 
	 * @param List<String> nodeIds
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	public void cleanTempTables(final List<String> nodeIds) throws SQLException, InterruptedException {
		
		final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections.synchronizedList(new ArrayList<SQLException>());
        
		for (final String nodeId : nodeIds) {
			tasks.add(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
                    try {
                    	ResultSet rs = TempTableCleaner.this.dbManager.fetch("" +
                    			"SELECT tablename " +
                    			"FROM   pg_catalog.pg_tables     " +
                    			"WHERE  tablename LIKE 'tmp_%'" , nodeId);
                    	StringBuilder sb = new StringBuilder();
                    	while(rs.next()) {
                    		sb.append("DROP TABLE " + rs.getString(1) + ";");
                    	}
                    	TempTableCleaner.this.dbManager.execute(sb.toString(), nodeId);
                    	

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
	 * This method shuts down the pool of thread used by this class
	 * It should be called before destructing the class 
	 * otherwise the main program will hang (wont exit)
	 */
	public void shutDown() {
        this.pool.shutdown();
	}
}
