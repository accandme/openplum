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
	private DatabaseManager dbManager;
	private final ExecutorService pool;
	
	public TempTableCleaner(DatabaseManager dbManager) {
		this.dbManager = dbManager;
		this.pool = Executors.newCachedThreadPool();
	}
	
	/**
	 * Cleans Temporary Tables
	 */
	public void cleanTempTables(final List<String> toNodeIds) throws SQLException, InterruptedException {
		
		final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections.synchronizedList(new ArrayList<SQLException>());
        
		for (final String nodeId : toNodeIds) {
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
	
	public void shutDown() {
        this.pool.shutdown();
	}
}
