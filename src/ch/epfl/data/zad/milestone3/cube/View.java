package ch.epfl.data.zad.milestone3.cube;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.epfl.data.distribdb.lowlevel.DatabaseManager;

/**
 * A view of the data cube (meta-data, no real data kept in memory).
 * 
 * @author tranbaoduy
 * @author Artyom Stetsenko
 * @author Anton Dimitrov
 * @author Amer C
 */
public class View {

    /**
     * Prefix of every view name.
     */
    private static final String VIEW_NAME_PREFIX = "view_";

    /**
     * Name of temp table that is created when data of some view is being
     * computed.
     */
    private static final String TEMP_VIEW_RESULT_TABLE = "temp_view_result";

    /**
     * Parent cube.
     */
    private Cube cube;

    /**
     * Aggregation state: Dimension name --> current hierarchy level
     */
    private Map<String, Integer> aggState;

    /**
     * Slice criteria. Map from Level Name to expression with $
     */
    private Map<String, String> sliceCriteria;

    /**
     * View's name that will be used as database table name if this view is
     * materialized.
     */
    private String name;

    /**
     * Whether this view has been materialized (available in DB).
     */
    private boolean materialized;

    /**
     * Holds information about the number of records needed for this view if
     * materialized.
     */
    private long storageCost;

    /**
     * Constructor: All dimensions at level 0, not default.
     * 
     * @param cube
     *            Parent cube of this view
     * @param dbManager
     *            Database manager
     * @param dimensions
     *            All dimension names --> dimension
     * @param factFields
     *            Fact fields
     */
    public View(Cube cube) {
        this(cube, null, null, null, false);
    }

    /**
     * Constructor.
     * 
     * @param cube
     *            Parent cube of this view
     * @param dbManager
     *            Database manager
     * @param dimensions
     *            All dimension names --> dimension
     * @param factFields
     *            Fact fields
     * @param sliceCriterion
     *            Slice criterion of this view
     * @param aggState
     *            Aggregation state (Dimension --> hierarchy level)
     */
    public View(Cube cube, Map<String, String> sliceCriterion, Map<String, Integer> aggState) {
        this(cube, sliceCriterion, aggState, null, false);
    }

    /**
     * Constructor.
     * 
     * @param cube
     *            Parent cube of this view
     * @param dbManager
     *            Database manager
     * @param dimensions
     *            All dimension names --> dimension
     * @param factFields
     *            Fact fields
     * @param sliceCriteria
     *            Slice criteria of this view
     * @param aggState
     *            Aggregation state (Dimension --> hierarchy level)
     * @param name
     *            Custom name of this view
     * @param materialized
     *            Flag indicating whether the view is materialized
     */
    public View(Cube cube, Map<String, String> sliceCriteria,
            Map<String, Integer> aggState, String name, boolean materialized) {

        if (cube == null) {
            throw new IllegalArgumentException("cube is null");
        }

        this.cube = cube;

        if (aggState == null) {
            aggState = new HashMap<String, Integer>();
            for (Dimension dim : this.cube.getDimensions().values()) {
                aggState.put(dim.getName(), 0);
            }
        }

        if (sliceCriteria == null) {
        	sliceCriteria = new HashMap<String, String>();
        }

        this.aggState = Collections.unmodifiableMap(aggState);
        this.sliceCriteria = Collections.unmodifiableMap(sliceCriteria);
        this.name = name;
        this.materialized = materialized;
        this.storageCost = 0;
    }

    /**
     * Returns this view's name, computing it on-the-fly, if necessary.
     * 
     * @return this view's name
     */
    public String getName() {

        if (this.name == null) {

            // Construct view name using a large random number
            this.name = View.VIEW_NAME_PREFIX + (new Date()).getTime();
        }

        return this.name;
    }

    /**
     * Gets this view's parent cube.
     * 
     * @return Parent cube
     */
    public Cube getParentCube() {
        return this.cube;
    }

    /**
     * Checks if this view has been materialized.
     * 
     * @return true if materialized, false otherwise
     */
    public boolean isMaterialized() {
        return this.materialized;
    }

    /**
     * Returns storage cost if computed already. Otherwise, computes it on the
     * fly.
     * 
     * @return The storage cost of the view.
     */
    public long getStorageCost() {
        return this.storageCost;
    }

    /**
     * Sets storage cost of the view.
     * 
     * @param storageCost
     *            The new cost.
     */
    public void setStorageCost(long storageCost) {
        this.storageCost = storageCost;
    }

    /**
     * Materialize this view in the DB.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void materialize() throws SQLException, InterruptedException {

        if (!this.materialized) {

            // View from which the current view can be computed
            View sourceView = this.cube.getClosestView(this);

            DatabaseManager dbManager = this.cube.getDatabaseManager();
            List<String> allNodes = dbManager.getNodeNames();

            // Drop existing table for this view, if any
            dbManager.execute(
                    String.format("DROP TABLE IF EXISTS %s", this.getName()),
                    allNodes);

            // Create final table for this view
            dbManager
                    .execute(this.getViewCreateQuery(this.getName()), allNodes);

            // Populate table for this view
            dbManager.execute(
                    "INSERT INTO "
                            + this.getName()
                            + " ("
                            + this.getViewSelectQuery(sourceView.getName(),
                                    false) + ")", allNodes);

            this.materialized = true;
        } 
    }

    /**
     * Dematerialize (drop) this view in the DB.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void dematerialize() throws SQLException, InterruptedException {

        if (this.materialized) {

            DatabaseManager dbManager = this.cube.getDatabaseManager();

            // Drop existing table for this view, if any
            dbManager.execute(
                    String.format("DROP TABLE IF EXISTS %s", this.getName()),
                    dbManager.getNodeNames());

            this.materialized = false;
        }
    }

    /**
     * Retrieves data of this view from the DB.
     * 
     * @return the data
     * @throws SQLException
     * @throws InterruptedException
     */
    public ResultSet getData() throws SQLException, InterruptedException {

        DatabaseManager dbManager = this.cube.getDatabaseManager();
        List<String> allNodes = dbManager.getNodeNames();

        // Node where temp table will be stored
        String tempNode = allNodes.get(0);

        // Drop existing temp table, if any
        dbManager.execute(String.format("DROP TABLE IF EXISTS %s",
                View.TEMP_VIEW_RESULT_TABLE), tempNode);

        // Create temp table
        dbManager.execute(this.getViewCreateQuery(View.TEMP_VIEW_RESULT_TABLE),
                tempNode);

        // Query to run on all nodes to get intermediary results
        String selectQuery;
        if (this.materialized) {
            selectQuery = String.format("SELECT * FROM %s", this.getName());
        } else {
            View sourceView = this.cube.getClosestView(this);
            selectQuery = this.getViewSelectQuery(sourceView.getName(), true);
        }

        // Populate temp table with results from each node
        dbManager.execute(selectQuery, allNodes, View.TEMP_VIEW_RESULT_TABLE,
                tempNode);

        // Get results
        ResultSet results = dbManager.fetch(
                this.getViewSelectQuery(View.TEMP_VIEW_RESULT_TABLE, false),
                tempNode);

        // Drop temp table
        dbManager.execute(String.format("DROP TABLE IF EXISTS %s",
                View.TEMP_VIEW_RESULT_TABLE), tempNode);

        return results;
    }

    /**
     * Gets the view's aggregation state (dimension name --> aggregate level in
     * dimension's hierarchy):
     * 
     * @return Aggregation state
     */
    public Map<String, Integer> getAggState() {
        return this.aggState;
    }

    /**
     * Gets the view's slice criterion.
     * 
     * @return Slice criterion
     */
    public Map<String, String> getSliceCriteria() {
        return this.sliceCriteria;
    }

    /**
     * Gets the query that needs to be executed to create a table with the view
     * schema. Note that a view is partitioned on all nodes and all views share
     * exactly the same schema.
     * 
     * @param tableName
     *            name of the table to create
     * @return resulting query
     */
    private String getViewCreateQuery(String tableName) {

        if (tableName == null || tableName.equals("")) {
            throw new IllegalArgumentException(
                    "Invalid name of table to create: " + tableName);
        }

        StringBuilder query = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        query.append(tableName).append(" (");

        String prefix = "";

        for (Dimension dim : this.cube.getDimensions().values()) {
            for (Level level : dim.getHierarchy()) {

                query.append(prefix);
                prefix = ", ";

                query.append(level.getName()).append(" CHAR(")
                        .append(level.getMaxValueSize()).append(")");
            }
        }

        for (FactField factField : this.cube.getFactFields()) {

            query.append(prefix);
            prefix = ", ";

            query.append(factField.getName()).append(" ");

            if (factField instanceof DoubleFactField) {

                DoubleFactField dff = (DoubleFactField) factField;

                query.append("DOUBLE(").append(dff.getPrecision()).append(", ")
                        .append(dff.getScale()).append(")");

            } else {

                query.append("INTEGER");
            }
        }

        return query.append(")").toString();
    }

    /**
     * Gets the query that needs to be executed to retrieve data for this view.
     * 
     * @param fromRelation
     *            relation that will be in the FROM clause of the query, needs
     *            to be a relation holding some view
     * @param includeSliceDice
     *            flag indicating whether the slice/dice criterion of this view
     *            (i.e. the WHERE clause) should be included in the query
     * @return resulting query
     */
    private String getViewSelectQuery(String fromRelation,
            boolean includeSliceDice) {

        if (fromRelation == null || fromRelation.equals("")) {
            throw new IllegalArgumentException(
                    "Invalid name of table to select from: " + fromRelation);
        }

        /* SELECT */

        StringBuilder query = new StringBuilder("SELECT ");

        String prefix = "";

        // Dimensions
        for (Dimension dim : this.cube.getDimensions().values()) {

            int dimDepth = this.aggState.get(dim.getName());

            // Levels that need to be shown
            for (int i = 0; i < dimDepth; i++) {
                query.append(prefix).append(dim.getName(i + 1));
                prefix = ", ";
            }

            // "ALL" levels
            for (int i = dimDepth; i < dim.getNumLevels() - 1; i++) {
                query.append(prefix).append(Level.ALL_PLACEHOLDER)
                        .append(" AS ").append(dim.getName(i + 1));
                prefix = ", ";
            }
        }

        // Facts
        for (FactField factField : this.cube.getFactFields()) {
            query.append(prefix).append("SUM(").append(factField.getName())
                    .append(") AS ").append(factField.getName());
            prefix = ", ";
        }

        /* FROM */

        query.append(" FROM ").append(fromRelation);

        /* WHERE */

        if (includeSliceDice) {
            String sliceSep = " WHERE ";
            for(String sliceLevel : this.sliceCriteria.keySet()) {
                query.append(sliceSep);
                query.append("(" + this.sliceCriteria.get(sliceLevel).replaceAll("[$]", sliceLevel) + ")");
                sliceSep = " AND ";
            }
        }

        /* GROUP BY */

        prefix = "";
        boolean groupByAppended = false;

        for (Dimension dim : this.cube.getDimensions().values()) {

            int dimDepth = this.aggState.get(dim.getName());

            if (dimDepth == 0)
                continue; // no aggregation on this dimension

            // Append "GROUP BY" to query only once (but maybe not at all if
            // this is topmost view)
            if (!groupByAppended) {
                query.append(" GROUP BY ");
                groupByAppended = true;
            }

            // List group-by levels of this view for this dimension
            for (int i = 1; i <= dimDepth; i++) {
                query.append(prefix).append(dim.getName(i));
                prefix = ", ";
            }

        }

        return query.toString();
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + this.aggState.hashCode();
        hash = hash * 31 + this.sliceCriteria.hashCode();
        return hash;
    }

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof View)) {
			return false;
		}
		final View that = (View) obj;
		return this.aggState.equals(that.aggState) && this.sliceCriteria.equals(that.sliceCriteria);
	}
	
}
