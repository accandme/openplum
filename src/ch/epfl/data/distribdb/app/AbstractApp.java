package ch.epfl.data.distribdb.app;

import java.io.FileInputStream;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import ch.epfl.data.distribdb.lowlevel.DatabaseManager;
import ch.epfl.data.distribdb.lowlevel.ParallelDatabaseManager;

public abstract class AbstractApp {
	
	public static final boolean DEBUG = false;

	protected List<String> allNodes = new ArrayList<String>();
	protected long storageLimitCost = 0;
	
	public abstract void run(String[] args) throws SQLException, InterruptedException;
	
	public DatabaseManager createDatabaseManager(String configFile) throws SQLException {
		DatabaseManager dbManager = new ParallelDatabaseManager();
		
    	Properties prop = new Properties();
    	 
    	try {
            //load a properties file
    		prop.load(new FileInputStream(configFile));
 
    		for (int i = 0; i < Integer.parseInt(prop.getProperty("numberOfNodes")); i++) {
    			
    			dbManager.connect(
    					"node" + i, 
    					prop.getProperty("node" + i + ".jdbc"), 
    					prop.getProperty("node" + i + ".username"), 
    					prop.getProperty("node" + i + ".password"));
    			
    			allNodes.add("node" + i);
    		}
    		
    		this.storageLimitCost = Long.parseLong(prop.getProperty("storageLimitCost"));
    	} catch (Exception ex) {
    		System.out.println("Invalid config file path or .properties file structure");
    		System.exit(1);
    	}
    	
		return dbManager;	
	}
	
	public String getFormattedFloat(String value) {
		double doubleValue = Double.parseDouble(value);
		DecimalFormat fourDec = new DecimalFormat("0.0000", new
				DecimalFormatSymbols(Locale.US));
		return fourDec.format(doubleValue);
	}
}
