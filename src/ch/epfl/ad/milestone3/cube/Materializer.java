package ch.epfl.ad.milestone3.cube;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.epfl.ad.db.DatabaseManager;

/**
 * Given the information in the fact table chooses which views to materialize.
 * Takes into consideration the storage cost limit supplied by the client.
 * 
 * @author Anton Dimitrov
 * @author Artyom Stetsenko
 */
public class Materializer {

	/**
	 * Name of the fact table view.
	 */
	private String factTableViewName;
	
	/**
	 * Name of the table holding info about levels' arities.
	 */
	private String aritiesTableName; 
	
    /**
     * Dimensions configuration.
     */
    Set<Dimension> dimensions;

    /**
     * Cube fact fields.
     */
    private List<FactField> factFields;

    /**
     * Database manager.
     */
    private DatabaseManager dbManager;

    /**
     * This cube is needed in order to reuse the logic for materializing views
     * in a smart way reusing other materialized views.
     */
    private Cube cube;

    /**
     * Limit on the maximum total storage cost of all materialized views.
     */
    private long storageCostLimit;

    /**
     * The name of the table holding information about the materialized views.
     */
    private String catalogTableName;

    /**
     * Name of a node to use for keeping the table with materialized views
     * information and also for storing temporary tables when getting dimensions
     * arities.
     */
    private String nodeName;
    
    /**
     * @param storageLimitCost
     * 			  The limit for storage cost when materializing views.
     * @param materializedViewsTableName
     * 			  The name of the table holding information about the 
     * 			  materialized views.
     * @param aritiesTableName
     * 			  The name of the table containing information about arities
     * 			  of all levels of all dimensions.
     * @param factTableViewName
     *            The name of the fact table view.
     * @param dims
     *            Dimensions of the fact table.
     * @param factFields
     *            Fact table description.
     * @param dbManager
     *            Database manager for accessing distributed database.
     * @param nodeName
     * 			  Name of the node which will be used to store the table
     * 			  with information about materialized views and also where
     * 			  temporary tables will be stored.
     */
    public Materializer(long storageLimitCost, String materializedViewsTableName, 
    		String aritiesTableName, String factTableViewName,
            Set<Dimension> dims, List<FactField> factFields,
            DatabaseManager dbManager, String nodeName) {
        this.storageCostLimit = storageLimitCost;
        this.catalogTableName = materializedViewsTableName;
        this.factTableViewName = factTableViewName;
        this.aritiesTableName = aritiesTableName;
        this.dimensions = dims;
        this.factFields = factFields;
        this.dbManager = dbManager;
        this.nodeName = nodeName;
    }

    private static String buildFullLevelName(String dimName, int levelIdx) {
        return String.format("%s_%d", dimName, levelIdx);
    }

    /**
     * Chooses which views to materialize for this cube. The decision is based
     * on the storage cost and usage cost of all views.
     * 
     * @throws InterruptedException
     * @throws SQLException
     */
    public boolean chooseAndMaterializeViews() throws SQLException,
            InterruptedException {

        
        // Compute arities for all levels in current data.
        System.out
                .println("Compute arities of all levels of all dimensions in data...");
        Map<String, Long> arities = computeArities();        

        System.out.println("Building materialized views...");
        
        this.cube = new Cube(this.factTableViewName, this.dimensions,
                this.factFields, this.dbManager);

        System.out.println("Find all views...");
        List<Dimension> dimList = new ArrayList<Dimension>();
        List<Integer> dimLevel = new ArrayList<Integer>();
        for (Dimension dim : this.dimensions) {
            dimList.add(dim);
            dimLevel.add(0);
        }

        List<View> generatedViews = new ArrayList<View>();
        this.recurseViews(0, dimList, dimLevel, generatedViews);

        // Compute storage costs for all views.
        System.out.println("Compute storage costs for views...");
        computeStorageCosts(generatedViews, arities);

        // Sort views by storage cost in descending order.
        System.out.println("Sort views by storage cost...");
        Collections.sort(generatedViews, new Comparator<View>() {
			public int compare(View o1, View o2) {
				if(o1.getStorageCost() > o2.getStorageCost())
					return 1;
				if(o1.getStorageCost() < o2.getStorageCost())
					return -1;
				return 0;
			}
		});
        
        List<View> qualifiedViews = new ArrayList<View>();
        long currLimit = this.storageCostLimit;
        int counter = 0;
        
        // Checks storage cost and adds qualified views in reverse order
        System.out.println("Choose views to materialize...");
        for (View view : generatedViews) {
            if (currLimit >= view.getStorageCost()) {
                currLimit -= view.getStorageCost();
                qualifiedViews.add(0, view);
                counter++;
            } else {
                break;
            }
        }

        System.out.print("Views chosen to materialize: ");
        System.out.println(counter);
        
        this.createCatalogTable();

        System.out.println("Materializing views...");
        for (View view : qualifiedViews) {
                
            view.materialize();
            this.cube.addMaterializedView(view);
                
            this.dbManager.execute(this.buildCatalogInsertQuery(view),
                    this.nodeName);
        }
        System.out.print("Done!");
        return true;
    }

    private void createCatalogTable() throws SQLException {
        
        // Drop existing table for marking chosen materialized views.
        this.dbManager.execute(String.format("DROP TABLE IF EXISTS %s",
                this.catalogTableName), this.nodeName);

        StringBuilder query = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        query.append(this.catalogTableName);
        query.append(" (");

        for (Dimension dim : this.dimensions) {
            query.append(dim.getName()).append(" INT, ");
        }
        query.append("table_name CHAR(255) )");
        this.dbManager.execute(query.toString(), this.nodeName);
    }

    private long getLevelArity(Dimension dim, int levelIndex) 
    		throws SQLException, InterruptedException {

    	long cost = -1;
    	
    	// Compute list of field names for the queries.
    	StringBuilder fieldsList = new StringBuilder(dim.getLevel(1).getName());
    	for (int i = 2; i <= levelIndex; i++) {
    		fieldsList.append(", ");
    		fieldsList.append(dim.getLevel(i).getName());
    	}
    	
		// Collect distinct field values from all nodes and send them to one
		// node.    	
        String query = String.format(
                "SELECT distinct %s from %s", fieldsList, this.factTableViewName);
        this.dbManager.execute(query, dbManager.getNodeNames(),
        		"temp_level_fields", this.nodeName);
        
        // Count the different field values.
        query = String.format(
                "SELECT count(distinct %s) from %s", fieldsList, this.factTableViewName);
        ResultSet result = this.dbManager.fetch(query, this.nodeName);
        if (result.next()) {
            cost = result.getInt(1);
        } else {
            System.err.println("Problem getting result of query: "
                    + query);
            throw new SQLException("Unable to get result from query: " + query);
        }

		this.dbManager.execute("DROP TABLE IF EXISTS temp_level_fields", this.nodeName);
    	return cost;
    }

    private Map<String, Long> computeArities() throws SQLException, InterruptedException {
    	
    	// Create the table which will store the arities of all levels.

    	// Drop existing table for marking chosen materialized views.
        this.dbManager.execute(String.format("DROP TABLE IF EXISTS %s",
                this.aritiesTableName), this.nodeName);

        StringBuilder query = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        query.append(this.aritiesTableName);
        query.append(" (level_name CHAR(255), arity INT)");
        this.dbManager.execute(query.toString(), this.nodeName);
    	
        Map<String, Long> arities = new HashMap<String, Long>();
        for (Dimension dim : this.dimensions) {
            for (int i = 1; i < dim.getNumLevels(); i++) {
                String fullLevelName = buildFullLevelName(dim.getName(), i);
                //long cost = getLevelArity(dim.getLevel(i).getName());
                long cost = getLevelArity(dim, i);
                System.out.println(String.format("Level %s -> %d", dim.getLevel(i).getName(), cost));
                arities.put(fullLevelName, cost);
                
                // Insert information about arity of this level in the table.
                this.dbManager.execute(String.format(
                		"INSERT INTO %s (level_name, arity) VALUES ('%s', %d)", 
                		this.aritiesTableName, dim.getLevel(i).getName(), cost), this.nodeName);
            }
        }

        return arities;
    }

    private String buildCatalogInsertQuery(View view) {
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(this.catalogTableName);
        StringBuilder fieldsList = new StringBuilder(" (");
        StringBuilder valuesList = new StringBuilder(" VALUES (");
        // Add columns for levels.
        Map<String, Integer> aggState = view.getAggState();
        for (String dimName : aggState.keySet()) {
            fieldsList.append(dimName);
            valuesList.append(aggState.get(dimName));
            fieldsList.append(", ");
            valuesList.append(", ");
        }
        // Add table name for this view.
        fieldsList.append("table_name)");
        valuesList.append("'" + view.getName() + "'");
        valuesList.append(")");
        query.append(fieldsList.toString());
        query.append(valuesList.toString());
        return query.toString();
    }

    private void computeStorageCosts(List<View> generatedViews,
            Map<String, Long> arities) {
        for (View view : generatedViews) {
            long cost = 1;
            for (Dimension dim : this.dimensions) {
                int dimLevel = view.getAggState().get(dim.getName());
                if (dimLevel > 0) {
                    String fullLevelName = buildFullLevelName(dim.getName(),
                            dimLevel);
                    Long levelCost = arities.get(fullLevelName);
                    if (levelCost != null) {
                        cost *= levelCost;
                    } else {
                        System.err.println("Problems getting arity for level "
                                + fullLevelName);
                    }
                }
            }
            view.setStorageCost(cost);
        }
    }

    private void recurseViews(int dimIdx, List<Dimension> dimList,
            List<Integer> dimLevel, List<View> generatedViews) {
        if (dimIdx == dimList.size()) {
            Map<String, Integer> aggState = new HashMap<String, Integer>();
            for (int i = 0; i < dimList.size(); i++) {
                aggState.put(dimList.get(i).getName(), dimLevel.get(i));
            }
            View tempView = new View(this.cube, null, aggState);
            generatedViews.add(tempView);
            return;
        }

        Dimension tempDim = dimList.get(dimIdx);
        // Leave the dimension with no aggregation.
        dimLevel.set(dimIdx, 0);
        this.recurseViews(dimIdx + 1, dimList, dimLevel, generatedViews);
        for (int i = 1; i < tempDim.getNumLevels(); i++) {
            // Set next level of aggregation and recurse to the
            // next dimension.
            dimLevel.set(dimIdx, i);
            this.recurseViews(dimIdx + 1, dimList, dimLevel, generatedViews);
        }
    }
    
    public static Set<View> getPreloadedViews(Cube cube,
            String catalogTableName, String catalogNodeId) throws SQLException {

        Set<View> preloadedViews = new HashSet<View>();
        ResultSet rs = cube.getDatabaseManager().fetch(
                "SELECT * FROM " + catalogTableName, catalogNodeId);

        while (rs.next()) {

            Map<String, Integer> aggState = new HashMap<String, Integer>();

            for (Dimension dim : cube.getDimensions().values()) {

                String dimName = dim.getName();
                aggState.put(dimName, rs.getInt(dimName));
            }

            preloadedViews.add(new View(cube, null, aggState, rs
                    .getString("table_name"), true));
        }
        
        return preloadedViews;
    }    

    public static Map<String, Integer> getArities(Cube cube,
            String catalogTableName, String catalogNodeId) throws SQLException {

        Map<String, Integer> arities = new HashMap<String, Integer>();
        ResultSet rs = cube.getDatabaseManager().fetch(
                "SELECT * FROM " + catalogTableName, catalogNodeId);

        while (rs.next()) {
        	arities.put(rs.getString("level_name"), rs.getInt("arity"));
        }
        
        return arities;
    }    

}
